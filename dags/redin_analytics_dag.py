from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import os
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from dotenv import load_dotenv

#loading env. variables to the environment 
load_dotenv('../.env')

#get environment variables
url_link = os.getenv('URL_LINK')
raw_output_file_path = os.getenv('RAW_DATA_PATH')
transform_output_file_path = os.getenv('TRANSFORMED_DATA_PATH')
s3_bucket_name = os.getenv('S3_BUCKET_NAME')


#extracting the refin data
def extract_data(**kwargs):
    """
    Function for extracting the data from redfin datacenter url and loading as csv.

    Args:
        url: Downloading link of the compressed data file

    Returns:
        List: returns the file name and the path of it in the system. 


    """
    url=kwargs['url']

    #create filename with the timestamp
    datenow_str = datetime.now().strftime('%d%m%Y%H%M%S')
    file_name = f'redin_data_{datenow_str}.csv'
    output_filepath = kwargs['op_filepath']
    full_path = os.path.join(output_filepath, file_name)

    #Extracting the data in chunks
    print("Started to process the file in chunks")
    first_chunk = True
    for chunk in pd.read_csv(url, compression='gzip', sep='\t', chunksize=3000):
        chunk.to_csv(
            full_path,
            mode='w' if first_chunk else 'a',
            index=False
        )

        first_chunk = False

    print("Finished extracting the files")
    output_list = [file_name, output_filepath]
    return output_list


def transform_data(**kwargs):
    """
    Function for transforming the extracted data and loading into AWS s3.

    Args:
        url: 

    Returns:
        List: returns the file name and the path of it in the system. 


    """

    #variables needed for the task
    file_info = kwargs['ti'].xcom_pull(task_ids='extract_data_redfin')
    file_name = file_info[0]
    file_path = file_info[1]
    output_file_path = kwargs['op_transformed_path']
    input_file = os.path.join(file_path, file_name)

    #create output file name
    transformed_file_name = file_name + '_trasformed_data.csv'
    output_file = os.path.join(output_file_path, transformed_file_name)

    #define columns to keep only
    cols = ['PERIOD_BEGIN','PERIOD_END','PERIOD_DURATION', 'REGION_TYPE', 'REGION_TYPE_ID', 'TABLE_ID',
            'IS_SEASONALLY_ADJUSTED', 'CITY', 'STATE', 'STATE_CODE', 'PROPERTY_TYPE', 'PROPERTY_TYPE_ID',
            'MEDIAN_SALE_PRICE', 'MEDIAN_LIST_PRICE', 'MEDIAN_PPSF', 'MEDIAN_LIST_PPSF', 'HOMES_SOLD',
            'INVENTORY', 'MONTHS_OF_SUPPLY', 'MEDIAN_DOM', 'AVG_SALE_TO_LIST', 'SOLD_ABOVE_LIST', 
            'PARENT_METRO_REGION_METRO_CODE', 'LAST_UPDATED']
    
    #Months mapping
    month_dict = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }



    print(f"Transforming the file {file_name}")

    #Transforming the data in chunks
    first_chunk = True
    for chunk in pd.read_csv(input_file, sep=',', chunksize=3000):
        
        #removing commas in city column
        chunk['CITY'] = chunk['CITY'].str.replace(',', '', regex=False)
        
        #select only needed columns
        chunk = chunk[cols]

        #removing null values
        chunk = chunk.dropna()

        #converting to datetime format 
        chunk['PERIOD_BEGIN'] = pd.to_datetime(chunk['PERIOD_BEGIN'], format='%Y-%m-%d', errors='coerce') #2023-10-01
        chunk['PERIOD_END'] = pd.to_datetime(chunk['PERIOD_END'], format='%Y-%m-%d', errors='coerce')
        
        #remove rows where datetime conversion failed (including header rows)
        chunk = chunk.dropna(subset=['PERIOD_BEGIN', 'PERIOD_END'])

        #creating new columns by extracting years, and months
        chunk['PERIOD_BEGIN_IN_YEARS'] = chunk['PERIOD_BEGIN'].dt.year
        chunk['PERIOD_BEGIN_IN_MONTHS'] = chunk['PERIOD_BEGIN'].dt.month
        chunk['PERIOD_END_IN_YEARS'] = chunk['PERIOD_END'].dt.year
        chunk['PERIOD_END_IN_MONTHS'] = chunk['PERIOD_END'].dt.month

        #Map months columns to month names
        chunk['PERIOD_BEGIN_IN_MONTHS'] = chunk['PERIOD_BEGIN_IN_MONTHS'].map(month_dict)
        chunk['PERIOD_END_IN_MONTHS'] = chunk['PERIOD_END_IN_MONTHS'].map(month_dict)

        #save transformed data
        chunk.to_csv(
            output_file,
            mode='w' if first_chunk else 'a',
            index=False,
            header=first_chunk
        )

        first_chunk=False

    print("Transformation complete")

    return [transformed_file_name, output_file]

def delete_existing_data(**kwargs):
    """
    Function that will delete the existing data in the s3 bucket for new data uplaoding
    """

    #creating s3 hook
    s3_hook = S3Hook(aws_conn_id='aws_connector')

    bucket = kwargs['bucket_name']

    #list files in the bucket
    keys = s3_hook.list_keys(bucket)

    #deleting the old files before loading the data
    s3_hook.delete_objects(
        bucket,
        keys
    )



def load_data(**kwargs):
    """
    Function for loading the raw and transformed data to AWS s3

    Args:
        url: 

    Returns:
        None: returns nothing
    """

    #file info 
    file_info = kwargs['ti'].xcom_pull(task_ids='transform_raw_data_load_s3')
    transform_file_name = file_info[0]
    transform_file_path = file_info[1]
    upload_path = os.path.join(transform_file_path, transform_file_name)

    #initialise s3 Hook with connection
    s3_hook = S3Hook(aws_conn_id='aws_connector')

    #bucket details
    bucket_name = kwargs['bucket_name']
    s3_key_transform = f"transformed_data/{transform_file_name}"

    #upload to s3 buckets
    s3_hook.load_file(
        upload_path,
        s3_key_transform,
        bucket_name,
        replace=True
    )


def delete_files(**kwargs):
    """
    Function for deleting the local raw and transformed data

    Args:
        :

    Returns:
        None: returns nothing
    """

    #file details retriving using task instance
    raw_file_info = kwargs['ti'].xcom_pull(task_ids='extract_data_redfin')
    transform_file_info = kwargs['ti'].xcom_pull(task_ids='transform_raw_data_load_s3')
    raw_file_path = os.path.join(raw_file_info[1], raw_file_info[0])
    transform_file_path = os.path.join(transform_file_info[0], transform_file_info[1])

    #removing the files
    os.remove(raw_file_path)
    os.remove(transform_file_path)



#creating DAG
with DAG(
    dag_id="redfin_analytics_dag",
    default_args={
        "depends_on_past": False,
        "retries": 3,
        "retry_delay": timedelta(minutes=5)
    },
    start_date=datetime(2025, 7, 10),
    schedule='@weekly' #scheduling weekly

) as dag:
    
    extract_redfin_data = PythonOperator(
        task_id="extract_data_redfin",
        python_callable=extract_data,  #python function to write and update
        op_kwargs={'url': url_link, 'op_filepath': raw_output_file_path}
    )

    transform_load_data = PythonOperator(
        task_id="transform_raw_data_load_s3",
        python_callable=transform_data,
        op_kwargs = {'op_transformed_path':transform_output_file_path}  #python function to write and update
    )

    delete_s3_data = PythonOperator(
        task_id = "delete_aws_s3_files",
        python_callable=delete_existing_data,
        op_kwargs={'bucket_name': s3_bucket_name} #python function to delete existing data
    )

    load_s3 = PythonOperator(
        task_id="load_data_s3",
        python_callable=load_data, # python function to load the data to s3
        op_kwargs={'bucket_name': s3_bucket_name}
    )

    delete_local_files = PythonOperator(
        task_id="delete_local_files",
        python_callable=delete_files #delete local new data
    )

    
    extract_redfin_data >> transform_load_data >> delete_s3_data >>load_s3 >> delete_local_files

