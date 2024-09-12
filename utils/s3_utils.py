import boto3
import pandas as pd
from io import StringIO, BytesIO
import gzip
import os
import logging
import csv
from botocore.exceptions import NoCredentialsError, ClientError

class S3:
    """
    S3 class for interacting with Amazon S3.

    This class provides methods to perform various operations on Amazon S3, including reading files into a DataFrame,
    uploading DataFrame to S3, copying files between local storage and S3, and managing logging for these operations.

    Attributes:
        logger (logging.Logger): Logger instance for logging messages.
        s3 (boto3.resource): Boto3 S3 resource instance.
        s3_client (boto3.client): Boto3 S3 client instance.

    Methods:
        __init__(self, aws_access_key_id, aws_secret_access_key, aws_session_token, region_name, staging, log_level):
            Initializes the S3 class with AWS credentials, region, and logging settings.
        
        read_s3_files_to_df(self, bucket_name, prefix):
            Reads files from an S3 bucket with the given prefix into a pandas DataFrame.
        
        _read_file_from_object(self, obj, key):
            Helper method to read different file types from an S3 object.
        
        set_log_level(self, log_level):
            Sets the logging level.
        
        upload_df_to_s3(self, dataframe, bucket_name, s3_key, index, quotechar, quoting, escapechar):
            Uploads a pandas DataFrame to S3 in CSV, gzipped CSV, or Parquet format.
        
        copy_to_s3(self, path, bucket_name, s3_prefix):
            Copies files or directories from local storage to S3.
        
        copy_to_local(self, bucket_name, s3_prefix, local_path):
            Copies files from S3 to local storage.
        
        _download_file(self, bucket_name, s3_key, local_path):
            Helper method to download a single file from S3 to local storage.
        
        _upload_file(self, file_path, bucket_name, s3_key):
            Helper method to upload a single file from local storage to S3.
        
        _upload_directory(self, directory_path, bucket_name, s3_prefix):
            Helper method to upload a directory from local storage to S3.
        
        _upload_csv(self, dataframe, bucket_name, s3_key, index, quotechar, quoting, escapechar):
            Helper method to upload a pandas DataFrame to S3 in CSV format.
        
        _upload_csv_gzip(self, dataframe, bucket_name, s3_key, index, quotechar, quoting, escapechar):
            Helper method to upload a pandas DataFrame to S3 in gzipped CSV format.
        
        _upload_parquet(self, dataframe, bucket_name, s3_key):
            Helper method to upload a pandas DataFrame to S3 in Parquet format.
        
        delete_objects_from_s3(self, bucket_name, s3_prefix):
            Deletes objects from an S3 bucket with the given prefix.
        
        list_objects_in_bucket(self, bucket_name, prefix, return_list):
            Lists objects in an S3 bucket with the given prefix.
        
        # Deprecated: list_all_permissions(self):
            # Lists all permissions for the current AWS IAM entity. (Commented out in the code as it might not be useful)
        #Sample Usage:
            lst = s3.list_objects_in_bucket('bl-data-staging', 'test', return_list=True)
            s3.delete_objects_from_s3('bl-data-staging', 'test/test.csv')
            #create a random pandas table with 2 columns
            import pandas as pd
            df = pd.DataFrame({'a': [1,2,3], 'b': [4,5,6]})
            s3.upload_df_to_s3(df, 'bl-data-staging', 'test/test.csv')
            df = s3.read_s3_files_to_df('bl-data-staging', 'test/test.csv')
        Other Examples:
            # Do this to get all valid functions within the class:
                [func for func in dir(S3) if callable(getattr(S3, func)) and not func.startswith("_")]
    """
    def __init__(self, aws_access_key_id=None, aws_secret_access_key=None, aws_session_token=None, region_name=None, staging=False,log_level=logging.INFO):
        # Set up logger
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(log_level)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        if not self.logger.handlers:
            self.logger.addHandler(handler)

        # Retrieve credentials from environment variables if not provided
        aws_access_key_id = aws_access_key_id or os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_access_key = aws_secret_access_key or os.getenv('AWS_SECRET_ACCESS_KEY')
        region_name = region_name or os.getenv('AWS_REGION')
    
        if staging:
            aws_session_token = aws_session_token or os.getenv('AWS_SESSION_TOKEN')
            self.s3 = boto3.resource(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
                aws_session_token=aws_session_token
            )
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name,
                aws_session_token=aws_session_token
            )
            #THIS MIGHT NOT BE USEFUL SINCE THE PORTAL METHOD SEEMS TO BE CREATING THE ARN DIFFERENTLY
            # self.sts_client = boto3.client(
            #     'sts',
            #     aws_access_key_id=aws_access_key_id,
            #     aws_secret_access_key=aws_secret_access_key,
            #     aws_session_token=aws_session_token,
            #     region_name=region_name
            # )
            # self.iam_client = boto3.client(
            #     'iam',
            #     aws_access_key_id=aws_access_key_id,
            #     aws_secret_access_key=aws_secret_access_key,
            #     aws_session_token=aws_session_token,
            #     region_name=region_name
            # )
        else:
            self.s3 = boto3.resource(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
            self.s3_client = boto3.client(
                's3',
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                region_name=region_name
            )
            #THIS MIGHT NOT BE USEFUL SINCE THE PORTAL METHOD SEEMS TO BE CREATING THE ARN DIFFERENTLY
            # self.sts_client = boto3.client(
            #     'sts',
            #     aws_access_key_id=aws_access_key_id,
            #     aws_secret_access_key=aws_secret_access_key,
            #     region_name=region_name
            # )
            # self.iam_client = boto3.client(
            #     'iam',
            #     aws_access_key_id=aws_access_key_id,
            #     aws_secret_access_key=aws_secret_access_key,
            #     region_name=region_name
            # )

    def read_s3_files_to_df(self, bucket_name, prefix):
        try:
            # Try to get the object metadata to check if it's a file
            try:
                obj_metadata = self.s3_client.head_object(Bucket=bucket_name, Key=prefix)
                is_file = True
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    is_file = False
                else:
                    raise e

            data_frames = []

            if is_file:
                self.logger.info(f"Processing file: {prefix}")
                obj = self.s3_client.get_object(Bucket=bucket_name, Key=prefix)
                data_frames.append(self._read_file_from_object(obj, prefix))
            else:
                bucket = self.s3.Bucket(bucket_name)
                files = bucket.objects.filter(Prefix=prefix)

                for file in files:
                    key = file.key
                    self.logger.info(f"Processing file: {key}")

                    obj = self.s3_client.get_object(Bucket=bucket_name, Key=key)
                    data_frames.append(self._read_file_from_object(obj, key))

            if data_frames:
                return pd.concat(data_frames, ignore_index=True)
            else:
                self.logger.warning("No valid files found to read.")
                return pd.DataFrame()

        except (NoCredentialsError, ClientError) as e:
            self.logger.error(f"Error reading S3 files: {str(e)}")
            return pd.DataFrame()

    def _read_file_from_object(self, obj, key):
        if key.endswith('.csv'):
            return pd.read_csv(obj['Body'])
        elif key.endswith('.csv.zip'):
            with gzip.GzipFile(fileobj=BytesIO(obj['Body'].read())) as gz:
                return pd.read_csv(gz)
        elif key.endswith('.parquet'):
            return pd.read_parquet(BytesIO(obj['Body'].read()))
        else:
            self.logger.warning(f"Unsupported file type: {key}")
            return pd.DataFrame()

    def set_log_level(self, log_level):
        self.logger.setLevel(log_level)
        for handler in self.logger.handlers:
            handler.setLevel(log_level)

    def upload_df_to_s3(self, dataframe, bucket_name, s3_key, index=False, quotechar='\'', quoting=csv.QUOTE_NONE, escapechar='\\'):
        try:
            if s3_key.endswith('.csv'):
                self._upload_csv(dataframe, bucket_name, s3_key, index, quotechar, quoting, escapechar)
            elif s3_key.endswith('.csv.gz'):
                self._upload_csv_gzip(dataframe, bucket_name, s3_key, index, quotechar, quoting, escapechar)
            elif s3_key.endswith('.parquet'):
                self._upload_parquet(dataframe, bucket_name, s3_key)
            else:
                raise ValueError(f"Unsupported file extension for s3_key: {s3_key}")
        except NoCredentialsError as e:
            self.logger.error(f"Failed to upload to S3 due to credentials error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            raise

    def copy_to_s3(self, path, bucket_name, s3_prefix):
        try:
            if os.path.isfile(path):
                s3_key = os.path.join(s3_prefix, os.path.basename(path))
                self._upload_file(path, bucket_name, s3_key)
            elif os.path.isdir(path):
                self._upload_directory(path, bucket_name, s3_prefix)
            else:
                self.logger.error(f"The path {path} is neither a file nor a directory")
        except NoCredentialsError as e:
            self.logger.error(f"Failed to copy to S3 due to credentials error: {e}")
        except ClientError as e:
            self.logger.error(f"Failed to copy to S3 due to client error: {e}")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

    def copy_to_local(self, bucket_name, s3_prefix, local_path):
        try:
            # Check if the given S3 path is a file
            try:
                obj_metadata = self.s3_client.head_object(Bucket=bucket_name, Key=s3_prefix)
                is_file = True
            except ClientError as e:
                if e.response['Error']['Code'] == '404':
                    is_file = False
                else:
                    raise e

            if is_file:
                self._download_file(bucket_name, s3_prefix, local_path)
            else:
                bucket = self.s3.Bucket(bucket_name)
                files = bucket.objects.filter(Prefix=s3_prefix)

                for file in files:
                    key = file.key
                    relative_path = os.path.relpath(key, s3_prefix)
                    local_file_path = os.path.join(local_path, relative_path)

                    if not os.path.exists(os.path.dirname(local_file_path)):
                        os.makedirs(os.path.dirname(local_file_path))

                    self.logger.info(f"Downloading file {key} to {local_file_path}")
                    bucket.download_file(key, local_file_path)
                    self.logger.info(f"File {key} downloaded to {local_file_path}")

        except NoCredentialsError as e:
            self.logger.error(f"Failed to copy from S3 due to credentials error: {e}")
        except ClientError as e:
            self.logger.error(f"Failed to copy from S3 due to client error: {e}")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

    def _download_file(self, bucket_name, s3_key, local_path):
        try:
            local_file_path = os.path.join(local_path, os.path.basename(s3_key))

            if not os.path.exists(os.path.dirname(local_file_path)):
                os.makedirs(os.path.dirname(local_file_path))

            self.logger.info(f"Downloading file {s3_key} to {local_file_path}")
            self.s3_client.download_file(bucket_name, s3_key, local_file_path)
            self.logger.info(f"File {s3_key} downloaded to {local_file_path}")

        except NoCredentialsError as e:
            self.logger.error(f"Failed to download from S3 due to credentials error: {e}")
        except ClientError as e:
            self.logger.error(f"Failed to download from S3 due to client error: {e}")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

    def _upload_file(self, file_path, bucket_name, s3_key):
        try:
            self.s3_client.upload_file(file_path, bucket_name, s3_key)
            self.logger.info(f"File {file_path} uploaded to {bucket_name}/{s3_key}")
        except FileNotFoundError:
            self.logger.error(f"The file {file_path} was not found")
        except NoCredentialsError:
            self.logger.error("Credentials not available")

    def _upload_directory(self, directory_path, bucket_name, s3_prefix):
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                s3_key = os.path.join(s3_prefix, os.path.relpath(file_path, directory_path))
                self._upload_file(file_path, bucket_name, s3_key)

    def _upload_csv(self, dataframe, bucket_name, s3_key, index, quotechar, quoting, escapechar):
        try:
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer, index=index, quotechar=quotechar, quoting=quoting, escapechar=escapechar)
            self.s3.Object(bucket_name, s3_key).put(Body=csv_buffer.getvalue())
            self.logger.info(f"Successfully uploaded CSV to {bucket_name}/{s3_key}")
        except NoCredentialsError as e:
            self.logger.error(f"Failed to upload CSV to S3 due to credentials error: {e}")
        except ClientError as e:
            self.logger.error(f"Failed to upload CSV to S3 due to client error: {e}")
        except Exception as e:
            self.logger.error(f"An error occurred while uploading CSV: {e}")

    def _upload_csv_gzip(self, dataframe, bucket_name, s3_key, index, quotechar, quoting, escapechar):
        try:
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer, index=index, quotechar=quotechar, quoting=quoting, escapechar=escapechar)
            gz_buffer = BytesIO()
            with gzip.GzipFile(fileobj=gz_buffer, mode='w') as gz_file:
                gz_file.write(csv_buffer.getvalue().encode('utf-8'))
            self.s3.Object(bucket_name, s3_key).put(Body=gz_buffer.getvalue())
            self.logger.info(f"Successfully uploaded gzipped CSV to {bucket_name}/{s3_key}")
        except NoCredentialsError as e:
            self.logger.error(f"Failed to upload gzipped CSV to S3 due to credentials error: {e}")
        except ClientError as e:
            self.logger.error(f"Failed to upload gzipped CSV to S3 due to client error: {e}")
        except Exception as e:
            self.logger.error(f"An error occurred while uploading gzipped CSV: {e}")

    def _upload_parquet(self, dataframe, bucket_name, s3_key):
        try:
            parquet_buffer = BytesIO()
            dataframe.to_parquet(parquet_buffer, index=False)
            self.s3.Object(bucket_name, s3_key).put(Body=parquet_buffer.getvalue())
            self.logger.info(f"Successfully uploaded Parquet to {bucket_name}/{s3_key}")
        except NoCredentialsError as e:
            self.logger.error(f"Failed to upload Parquet to S3 due to credentials error: {e}")
        except ClientError as e:
            self.logger.error(f"Failed to upload Parquet to S3 due to client error: {e}")
        except Exception as e:
            self.logger.error(f"An error occurred while uploading Parquet: {e}")

    def delete_objects_from_s3(self, bucket_name, s3_prefix):
        try:
            # List all objects with the given prefix
            response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)
            if 'Contents' in response:
                # Collect all object keys to delete
                objects_to_delete = [{'Key': obj['Key']} for obj in response['Contents']]
                
                # Perform the batch delete operation
                delete_response = self.s3_client.delete_objects(
                    Bucket=bucket_name,
                    Delete={'Objects': objects_to_delete}
                )
                
                # Log the deleted objects
                if 'Deleted' in delete_response:
                    for deleted in delete_response['Deleted']:
                        self.logger.info(f"Successfully deleted {bucket_name}/{deleted['Key']}")
                else:
                    self.logger.info(f"No objects were deleted from {bucket_name} with prefix '{s3_prefix}'")
            else:
                self.logger.info(f"No objects found in {bucket_name} with prefix '{s3_prefix}'")
        except NoCredentialsError as e:
            self.logger.error(f"Failed to delete from S3 due to credentials error: {e}")
        except ClientError as e:
            self.logger.error(f"Failed to delete from S3 due to client error: {e}")
        except Exception as e:
            self.logger.error(f"An error occurred while deleting from S3: {e}")


    def list_objects_in_bucket(self, bucket_name, prefix='', return_list=True):
        try:
            response = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            list_output = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    self.logger.info(obj['Key'])
                    if return_list:
                        list_output.append(obj['Key'])
            if return_list:
                return list_output
            else:
                self.logger.info(f"No objects found in {bucket_name} with prefix '{prefix}'")
        except NoCredentialsError as e:
            self.logger.error(f"Failed to list objects in S3 due to credentials error: {e}")
        except ClientError as e:
            self.logger.error(f"Failed to list objects in S3 due to client error: {e}")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

    # THIS MIGHT NOT BE USEFUL SINCE THE PORTAL METHOD SEEMS TO BE CREATING THE ARN DIFFERENTLY
    # def list_all_permissions(self):
    #     try:
    #         # Get the current caller identity
    #         identity = self.sts_client.get_caller_identity()
    #         self.logger.info(f"Caller Identity: {identity}")
            
    #         # Get the ARN of the caller
    #         arn = identity['Arn']
    #         self.logger.info(f"ARN: {arn}")
            
    #         # Extract the IAM user or role name from the ARN
    #         if arn.startswith("arn:aws:iam::"):
    #             if ":user/" in arn:
    #                 entity_type = "User"
    #                 entity_name = arn.split(":user/")[1]
    #             elif ":role/" in arn:
    #                 entity_type = "Role"
    #                 entity_name = arn.split(":role/")[1]
    #             else:
    #                 self.logger.info(f"Unsupported ARN format, return ARN for troubleshooting: {arn}")
    #                 raise ValueError("Unsupported ARN format")
                
    #             # Simulate the policies for the IAM entity
    #             response = self.iam_client.simulate_principal_policy(
    #                 PolicySourceArn=arn,
    #                 ActionNames=['*']
    #             )
    #             self.logger.info(f"Simulated policies for {entity_type} '{entity_name}':")
    #             for result in response['EvaluationResults']:
    #                 self.logger.info(f"Action: {result['EvalActionName']}, Allowed: {result['EvalDecision']}")
    #         else:
    #             self.logger.info(f"Unsupported ARN format, return ARN for troubleshooting: {arn}")
    #             raise ValueError("Unsupported ARN format")
        
    #     except NoCredentialsError as e:
    #         self.logger.error(f"Failed to list permissions due to credentials error: {e}")
    #     except ClientError as e:
    #         self.logger.error(f"Failed to list permissions due to client error: {e}")
    #     except Exception as e:
    #         self.logger.error(f"An error occurred: {e}")