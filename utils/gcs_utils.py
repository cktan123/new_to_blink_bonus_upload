from google.cloud import storage
import pandas as pd
from io import StringIO, BytesIO
import gzip
import os
import logging
import csv

class GCS:
    """
    GCS class for interacting with Google Cloud Storage.

    This class provides methods to perform various operations on GCS, including reading files into a DataFrame,
    uploading DataFrame to GCS, copying files between local storage and GCS, and managing logging for these operations.

    Attributes:
        logger (logging.Logger): Logger instance for logging messages.
        client (storage.Client): Google Cloud Storage client instance.

    Methods:
        __init__(self, google_credential_path, log_level):
            Initializes the GCS class with Google Cloud credentials and logging settings.
        
        read_gcs_files_to_df(self, bucket_name, prefix):
            Reads files from a GCS bucket with the given prefix into a pandas DataFrame.
        
        _read_file_from_blob(self, blob):
            Helper method to read different file types from a GCS blob.
        
        set_log_level(self, log_level):
            Sets the logging level.
        
        upload_df_to_gcs(self, dataframe, bucket_name, gcs_key, index, quotechar, quoting, escapechar):
            Uploads a pandas DataFrame to GCS in CSV, gzipped CSV, or Parquet format.
        
        copy_to_gcs(self, path, bucket_name, gcs_prefix):
            Copies files or directories from local storage to GCS.
        
        copy_to_local(self, bucket_name, gcs_prefix, local_path):
            Copies files from GCS to local storage.
        
        _download_file(self, bucket_name, gcs_key, local_path):
            Helper method to download a single file from GCS to local storage.
        
        _upload_file(self, file_path, bucket_name, gcs_key):
            Helper method to upload a single file from local storage to GCS.
        
        _upload_directory(self, directory_path, bucket_name, gcs_prefix):
            Helper method to upload a directory from local storage to GCS.
        
        _upload_csv(self, dataframe, bucket_name, gcs_key, index, quotechar, quoting, escapechar):
            Helper method to upload a pandas DataFrame to GCS in CSV format.
        
        _upload_csv_gzip(self, dataframe, bucket_name, gcs_key, index, quotechar, quoting, escapechar):
            Helper method to upload a pandas DataFrame to GCS in gzipped CSV format.
        
        _upload_parquet(self, dataframe, bucket_name, gcs_key):
            Helper method to upload a pandas DataFrame to GCS in Parquet format.
        
        delete_objects_from_gcs(self, bucket_name, gcs_prefix):
            Deletes objects from a GCS bucket with the given prefix.
        
        list_objects_in_bucket(self, bucket_name, prefix, return_list):
            Lists objects in a GCS bucket with the given prefix.
    """
    
    def __init__(self, google_credential_path, log_level=logging.INFO):
        # Set up logger
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(log_level)
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        if not self.logger.handlers:
            self.logger.addHandler(handler)
        
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = google_credential_path
        self.client = storage.Client()

    def read_gcs_files_to_df(self, bucket_name, prefix):
        try:
            bucket = self.client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            data_frames = []
            
            for blob in blobs:
                self.logger.info(f"Processing file: {blob.name}")
                data_frames.append(self._read_file_from_blob(blob))
                
            if data_frames:
                return pd.concat(data_frames, ignore_index=True)
            else:
                self.logger.warning("No valid files found to read.")
                return pd.DataFrame()

        except Exception as e:
            self.logger.error(f"Error reading GCS files: {str(e)}")
            return pd.DataFrame()

    def _read_file_from_blob(self, blob):
        if blob.name.endswith('.csv'):
            content = blob.download_as_text()
            return pd.read_csv(StringIO(content))
        elif blob.name.endswith('.csv.gz'):
            content = gzip.decompress(blob.download_as_bytes())
            return pd.read_csv(BytesIO(content))
        elif blob.name.endswith('.parquet'):
            content = blob.download_as_bytes()
            return pd.read_parquet(BytesIO(content))
        else:
            self.logger.warning(f"Unsupported file type: {blob.name}")
            return pd.DataFrame()

    def set_log_level(self, log_level):
        self.logger.setLevel(log_level)
        for handler in self.logger.handlers:
            handler.setLevel(log_level)

    def upload_df_to_gcs(self, dataframe, bucket_name, gcs_key, index=False, quotechar='\'', quoting=csv.QUOTE_NONE, escapechar='\\'):
        try:
            if gcs_key.endswith('.csv'):
                self._upload_csv(dataframe, bucket_name, gcs_key, index, quotechar, quoting, escapechar)
            elif gcs_key.endswith('.csv.gz'):
                self._upload_csv_gzip(dataframe, bucket_name, gcs_key, index, quotechar, quoting, escapechar)
            elif gcs_key.endswith('.parquet'):
                self._upload_parquet(dataframe, bucket_name, gcs_key)
            else:
                raise ValueError(f"Unsupported file extension for gcs_key: {gcs_key}")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")
            raise

    def copy_to_gcs(self, path, bucket_name, gcs_prefix):
        try:
            if os.path.isfile(path):
                gcs_key = os.path.join(gcs_prefix, os.path.basename(path))
                self._upload_file(path, bucket_name, gcs_key)
            elif os.path.isdir(path):
                self._upload_directory(path, bucket_name, gcs_prefix)
            else:
                self.logger.error(f"The path {path} is neither a file nor a directory")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

    def copy_to_local(self, bucket_name, gcs_prefix, local_path):
        try:
            bucket = self.client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=gcs_prefix)

            for blob in blobs:
                relative_path = os.path.relpath(blob.name, gcs_prefix)
                local_file_path = os.path.join(local_path, relative_path)

                if not os.path.exists(os.path.dirname(local_file_path)):
                    os.makedirs(os.path.dirname(local_file_path))

                self.logger.info(f"Downloading file {blob.name} to {local_file_path}")
                blob.download_to_filename(local_file_path)
                self.logger.info(f"File {blob.name} downloaded to {local_file_path}")

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

    def _download_file(self, bucket_name, gcs_key, local_path):
        try:
            local_file_path = os.path.join(local_path, os.path.basename(gcs_key))

            if not os.path.exists(os.path.dirname(local_file_path)):
                os.makedirs(os.path.dirname(local_file_path))

            self.logger.info(f"Downloading file {gcs_key} to {local_file_path}")
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(gcs_key)
            blob.download_to_filename(local_file_path)
            self.logger.info(f"File {gcs_key} downloaded to {local_file_path}")

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

    def _upload_file(self, file_path, bucket_name, gcs_key):
        try:
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(gcs_key)
            blob.upload_from_filename(file_path)
            self.logger.info(f"File {file_path} uploaded to {bucket_name}/{gcs_key}")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

    def _upload_directory(self, directory_path, bucket_name, gcs_prefix):
        for root, _, files in os.walk(directory_path):
            for file in files:
                file_path = os.path.join(root, file)
                gcs_key = os.path.join(gcs_prefix, os.path.relpath(file_path, directory_path))
                self._upload_file(file_path, bucket_name, gcs_key)

    def _upload_csv(self, dataframe, bucket_name, gcs_key, index, quotechar, quoting, escapechar):
        try:
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer, index=index, quotechar=quotechar, quoting=quoting, escapechar=escapechar)
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(gcs_key)
            blob.upload_from_string(csv_buffer.getvalue(), content_type='text/csv')
            self.logger.info(f"Successfully uploaded CSV to {bucket_name}/{gcs_key}")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

    def _upload_csv_gzip(self, dataframe, bucket_name, gcs_key, index, quotechar, quoting, escapechar):
        try:
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer, index=index, quotechar=quotechar, quoting=quoting, escapechar=escapechar)
            gz_buffer = BytesIO()
            with gzip.GzipFile(fileobj=gz_buffer, mode='w') as gz_file:
                gz_file.write(csv_buffer.getvalue().encode('utf-8'))
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(gcs_key)
            blob.upload_from_string(gz_buffer.getvalue(), content_type='application/gzip')
            self.logger.info(f"Successfully uploaded gzipped CSV to {bucket_name}/{gcs_key}")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

    def _upload_parquet(self, dataframe, bucket_name, gcs_key):
        try:
            parquet_buffer = BytesIO()
            dataframe.to_parquet(parquet_buffer, index=False)
            bucket = self.client.bucket(bucket_name)
            blob = bucket.blob(gcs_key)
            blob.upload_from_string(parquet_buffer.getvalue(), content_type='application/octet-stream')
            self.logger.info(f"Successfully uploaded Parquet to {bucket_name}/{gcs_key}")
        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

    def delete_objects_from_gcs(self, bucket_name, gcs_prefix):
        try:
            bucket = self.client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=gcs_prefix)

            for blob in blobs:
                self.logger.info(f"Deleting file {blob.name}")
                blob.delete()
                self.logger.info(f"File {blob.name} deleted")

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

    def list_objects_in_bucket(self, bucket_name, prefix='', return_list=True):
        try:
            bucket = self.client.bucket(bucket_name)
            blobs = bucket.list_blobs(prefix=prefix)
            list_output = []
            
            for blob in blobs:
                self.logger.info(blob.name)
                if return_list:
                    list_output.append(blob.name)
                    
            if return_list:
                return list_output
            else:
                self.logger.info(f"No objects found in {bucket_name} with prefix '{prefix}'")

        except Exception as e:
            self.logger.error(f"An error occurred: {e}")

# Sample usage:
# gcs = GCS(google_credential_path='/home/chunkit/codebase/blink-data-warehouse-fb84cc3e005f.json')
# df = gcs.read_gcs_files_to_df('my-bucket', 'prefix/')
# gcs.upload_df_to_gcs(df, 'my-bucket', 'path/to/upload.csv')
