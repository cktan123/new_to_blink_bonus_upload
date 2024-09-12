import pandas as pd
import boto3
from io import BytesIO
import gzip
from utils.setting import AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION_NAME, AWS_SESSION_TOKEN

class S3:
    def __init__(self, bucket_name, aws_access_key_id=None, aws_secret_access_key=None, region_name=None):
        self.bucket_name = bucket_name
        self.aws_access_key_id = aws_access_key_id or AWS_ACCESS_KEY_ID
        self.aws_secret_access_key = aws_secret_access_key or AWS_SECRET_ACCESS_KEY
        self.region_name = region_name or AWS_REGION_NAME
        self.token = AWS_SESSION_TOKEN

        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
            aws_session_token=AWS_SESSION_TOKEN,
            region_name=self.region_name
        )

    def upload_df_to_s3(self, dataframe, s3_file_path):
        # Convert DataFrame to CSV and then compress with gzip
        csv_buffer = BytesIO()
        with gzip.GzipFile(fileobj=csv_buffer, mode='w') as gz:
            dataframe.to_csv(gz, index=False)
        
        # Reset buffer position to the beginning
        csv_buffer.seek(0)
        
        # Upload the compressed CSV to S3
        self.s3_client.upload_fileobj(csv_buffer, self.bucket_name, s3_file_path)

    def read_s3_csv_to_df(self, s3_file_path):
        # Download the file from S3
        csv_buffer = BytesIO()
        self.s3_client.download_fileobj(self.bucket_name, s3_file_path, csv_buffer)
        
        # Reset buffer position to the beginning
        csv_buffer.seek(0)
        
        # Read the CSV from buffer check if it's gz or not
        try:
            with gzip.GzipFile(fileobj=csv_buffer, mode='r') as gz:
                df = pd.read_csv(gz)
        except OSError:
            csv_buffer.seek(0)
            df = pd.read_csv(csv_buffer)
        
        return df
    def list_files_in_s3(self, prefix):
        response = self.s3_client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)

        # Check if 'Contents' key is in the response
        if 'Contents' in response:
            return [content['Key'] for content in response['Contents']]
        else:
            return []
    def identify_latest_date_from_partitioned(self, path):
        #compare s3 tables latest entries
        from utils.s3_utils import S3
        import pendulum

        done_paths = self.list_files_in_s3('tx_txn/type=issue/')
        #filter only string ends with /0.csv, the slash is literal and not an escape
        done_paths = [path for path in done_paths if path.endswith('/0.csv')]

        if done_paths:
            #get the max date of the latest file
            done_paths.sort(reverse=True)
            latest_path = done_paths[0]
            #extract year from year=
            year = latest_path.split('year=')[1].split('/')[0]
            #extract month from month=
            month = latest_path.split('month=')[1].split('/')[0]
            #extract day from day=
            day = latest_path.split('day=')[1].split('/')[0]
            #construct a date string out of it
            date = f"{year}-{month}-{day}"

            latest_entry_in_s3 = pendulum.from_format(date, 'YYYY-M-D')
            return latest_entry_in_s3.to_date_string()
        else:
            print("path doesn't existed, using default start_date:: 2024-07-02")
            return "2024-07-02"
# Example usage
# if __name__ == "__main__":
#     # Create a sample DataFrame
#     data = {
#         'Column1': [1, 2, 3, 4],
#         'Column2': ['A', 'B', 'C', 'D']
#     }
#     df = pd.DataFrame(data)

#     # S3 configuration
#     BUCKET_NAME = 'your-bucket-name'
#     S3_FILE_PATH = 'path/to/your/file.csv.gz'

#     # Create an instance of S3Uploader
#     uploader = S3Uploader(BUCKET_NAME)

#     # Upload the DataFrame to S3
#     uploader.upload_df_to_s3(df, S3_FILE_PATH)
