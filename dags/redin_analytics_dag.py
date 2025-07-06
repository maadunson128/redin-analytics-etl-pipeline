from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
import pandas as pd
import os

#url link from redin data center
url_link = "https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_market_tracker/city_market_tracker.tsv000.gz"


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
    file_name = f'redin_data_{datenow_str}'
    output_filepath = '/home/maadunson/airflow_project/data/'
    full_path = os.path.join(output_filepath, file_name)

    #Extracting the data in chunks
    print("Started to process the file in chunks")
    first_chunk = True
    for chunk in pd.read_csv(url, compression='gzip', sep='\t', chunksize=5000):
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

    ti = kwargs['ti']
    file_info = ti.xcom_pull(task_ids='extract_data_redfin')

    file_name, file_path = file_info
    input_file = os.path.join(file_path, file_name)

    #create output file name
    transformed_file_name = file_name.replace('.csv, _transformed.csv')
    output_file = os.path.join(file_path, file_name)

    #define columns to keep only
    cols = ['period_begin','period_end','period_duration', 'region_type', 'region_type_id', 'table_id',
            'is_seasonally_adjusted', 'city', 'state', 'state_code', 'property_type', 'property_type_id',
            'median_sale_price', 'median_list_price', 'median_ppsf', 'median_list_ppsf', 'homes_sold',
            'inventory', 'months_of_supply', 'median_dom', 'avg_sale_to_list', 'sold_above_list', 
            'parent_metro_region_metro_code', 'last_updated']
    
    #Months mapping
    month_dict = {
        1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
        7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"
    }



    print(f"Transforming the file {file_name}")

    #Transforming the data in chunks
    first_chunk = True
    for chunk in pd.read_csv(input_file, chunksize=3000):
        
        #removing commas in city column
        chunk['city'] = chunk['city'].str.replace(',', '', regex=False)
        
        #select only needed columns
        chunk = chunk[cols]

        #removing null values
        chunk = chunk.dropna()

        #converting to datetime format 
        chunk['period_begin'] = pd.to_datetime(chunk['period_begin'], errors='coerce')
        chunk['period_end'] = pd.to_datetime(chunk['period_end'], errors='coerce')

        #creating new columns by extracting years, and months
        chunk['period_begin_in_years'] = chunk['period_begin'].dt.year
        chunk['period_begin_in_months'] = chunk['period_begin'].dt.month
        chunk['period_end_in_years'] = chunk['period_end'].dt.year
        chunk['period_end_in_months'] = chunk['period_end'].dt.month

        #Map months columns to month names
        chunk['period_begin_in_months'] = chunk['period_begin_in_months'].map(month_dict)
        chunk['period_end_in_months'] = chunk['period_end_in_months'].map(month_dict)

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



def delete_files():
    pass

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
        op_kwargs={'url': url_link}
    )

    transform_load_data = PythonOperator(
        task_id="transform_raw_data_load_s3",
        python_callable=transform_data  #python function to write and update
    )

    # load_s3 = BashOperator(
    #     task_id="load_data_s3",
    #     bash_command="path_to_script.sh"  # script to write and update here, also it first deletes the data in the s3 bucket
    # )

    delete_local_files = PythonOperator(
        task_id="delete_local_files",
        python_callable=delete_files
    )

    



    # extract_redfin_data >> transform_load_data >> load_raw_s3

