def test_bq_connection():
    from google.cloud import bigquery
    import pandas as pd

    # Path to your service account key file
    service_account_path = "/home/chunkit/codebase/blink-data-warehouse-fb84cc3e005f.json"

    # Create a BigQuery client
    try:
        client = bigquery.Client.from_service_account_json(service_account_path)
        print("BigQuery client created successfully.")
    except Exception as e:
        print(f"Failed to create BigQuery client: {e}")

    # List datasets in your project to test the connection
    try:
        datasets = list(client.list_datasets())
        if datasets:
            print("Datasets in project:")
            for dataset in datasets:
                print(dataset.dataset_id)
        else:
            print("No datasets found or unable to connect.")
    except Exception as e:
        print(f"Failed to list datasets: {e}")

    # Optional: Run a simple query to further verify the connection
    try:
        query = "SELECT 1 AS test_column"
        query_job = client.query(query)
        results = query_job.result()
        for row in results:
            print(f"Query test result: {row.test_column}")
        print("Query executed successfully.")
    except Exception as e:
        print(f"Failed to execute query: {e}")

def bq_to_pd(query,cred = "/home/chunkit/codebase/blink-data-warehouse-fb84cc3e005f.json", limit = 1000):
    from google.cloud import bigquery
    import pandas as pd

    # Initialize a BigQuery client
    # Path to your service account key file
    service_account_path = "/home/chunkit/codebase/blink-data-warehouse-fb84cc3e005f.json"
    client = bigquery.Client.from_service_account_json(service_account_path)
    
    # Preprocess query
    # if query ends with "limit interger", do not append limit
    if limit != None:
        query = query + f" LIMIT {limit}"

    query_job = client.query(query)

    # Convert the query result to a pandas DataFrame
    df = query_job.to_dataframe()
    return df

# def bq_to_pd_v2(query,cred = "/home/chunkit/codebase/blink-data-warehouse-fb84cc3e005f.json", limit = 1000):
#     from google.cloud import bigquery
#     from google.cloud import bigquery_storage
#     import pandas as pd

#     # Initialize a BigQuery client
#     # Path to your service account key file
#     service_account_path = "/home/chunkit/codebase/blink-data-warehouse-fb84cc3e005f.json"
#     client = bigquery.Client.from_service_account_json(service_account_path)
    

#     bqstorageclient = bigquery_storage.BigQueryReadClient()

#     # Execute the query and download the results
#     query_job = client.query(query)

#     # Use the BigQuery Storage API to read the results
#     results = query_job.result().to_dataframe(bqstorage_client=bqstorageclient)
#     return results

def bq_to_pd_v2(query, cred="/home/chunkit/codebase/blink-data-warehouse-fb84cc3e005f.json"):
    from google.cloud import bigquery
    from google.cloud import bigquery_storage
    import pandas as pd
    from google.oauth2 import service_account

    # Load the credentials from the service account file
    credentials = service_account.Credentials.from_service_account_file(cred)

    # Initialize a BigQuery client using the credentials
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Initialize BigQuery Storage client using the same credentials
    bqstorageclient = bigquery_storage.BigQueryReadClient(credentials=credentials)

    # Execute the query and download the results
    query_job = client.query(query)

    # Use the BigQuery Storage API to read the results
    results = query_job.result().to_dataframe(bqstorage_client=bqstorageclient)
    return results


def generate_date_list(start_date_str, end_date_str, date_format="YYYY-MM-DD"):
    import pendulum
    # Parse the date strings into Pendulum datetime objects
    start_date = pendulum.from_format(start_date_str, date_format)
    end_date = pendulum.from_format(end_date_str, date_format)

    # Generate the list of dates between start_date and end_date
    date_list = []
    current_date = start_date

    while current_date <= end_date:
        date_list.append(current_date.to_date_string())
        current_date = current_date.add(days=1)

    # Convert the list to a tuple
    date_tuple = tuple(date_list)
    return date_tuple

def initiate_local_dask(spill_directory= "/home/chunkit/dask-tmp", memory_limit="8GB"):
    import pandas as pd
    from dask.distributed import Client, LocalCluster
    import os
    # Determine the total number of available threads on the system
    total_threads = os.cpu_count()

    # Define the number of threads you want to reserve for other tasks
    reserve_threads = 2  # For example, reserve 2 threads

    # Calculate the number of threads to use for Dask
    dask_threads = total_threads - reserve_threads
    # Initialize the Dask Client with the specified number of threads
    # Configure a local cluster sith specific memory limits
    spill_directory = spill_directory
    cluster = LocalCluster(
        n_workers=dask_threads,  # Number of workers
        threads_per_worker= 1,  # Threads per worker
        memory_limit=memory_limit,  # Memory limit per worker
        local_directory=spill_directory
    )

    client = Client(cluster)

    scheduler_address = client.scheduler_info()['address']

    # Write the scheduler address to a file
    with open("/home/chunkit/codebase/dask_address_file.txt", 'w') as f:
        f.write(scheduler_address)
    return client

def check_terminate_dask_local():
    import os
    from dask.distributed import Client, LocalCluster
    from distributed import Scheduler, rpc
    import socket

    # Function to check if a port is in use
    def is_port_in_use(port):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(('localhost', port)) == 0

    # Function to terminate an existing Dask cluster
    def terminate_cluster(port):
        if is_port_in_use(port):
            with rpc('tcp://localhost:%d' % port) as s:
                s.terminate(close_workers=True)

    # Ports to be used
    scheduler_port = 8786
    dashboard_port = 8787

    # Terminate any existing Dask cluster on the specified scheduler port
    terminate_cluster(scheduler_port)
    
def pd_to_bq(table_name, dataframe, cred="/home/chunkit/codebase/blink-data-warehouse-fb84cc3e005f.json"):
    from google.cloud import bigquery
    import pandas as pd

    # Initialize a BigQuery client
    # Path to your service account key file
    service_account_path = cred  # Use the provided credential path
    client = bigquery.Client.from_service_account_json(service_account_path)

    # Set the table_id to the ID of the destination table
    table_id = f"blink-data-warehouse.application_layer.{table_name}"

    # Create a job_config and set the write_disposition to 'WRITE_TRUNCATE' to overwrite existing data
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # Load the dataframe to BigQuery using the job_config
    job = client.load_table_from_dataframe(dataframe, table_id, job_config=job_config)

    # Wait for the load job to complete
    job.result()

    print(f"Dataframe loaded to BigQuery table {table_id}, overwriting existing data")
