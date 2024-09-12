class GCS:
    """
    Sample usage:
        # sample usage with pandas
        # data = {
        #     'column1': [1, 2, 3, 4, 5],
        #     'column2': ['a', 'b', 'c', 'd', 'e']
        # }
        # df = pd.DataFrame(data)
        # gcs = GCS()
        # gcs.upload_parquet_gcs(df, "ck/tmp/test/sample.parquet")


        # sample usage with dask and gcsfs
        # gcs = GCS()
        # df = gcs.read_partitioned_parquet_gcs("ck/tmp/test/")

        # df["column3"] = 'sample'

        # gcs.send_dask_dataframe_to_gcs(df, "ck/tmp/test/sample")
        # _ = gcs.read_partitioned_parquet_gcs("ck/tmp/test/sample")
    """
    def __init__(self, bucket_name="blink-data-staging-tmp"):
        from google.cloud import storage
        self.client = storage.Client()
        self.bucket_name = bucket_name
        self.bucket = self.client.get_bucket(bucket_name)
    
    def send_parquet_gcs(self, df, gcs_file_path):
        import io
        import pyarrow as pa
        import pyarrow.parquet as pq
        #upload as parquet
        buffer = io.BytesIO()
        table = pa.Table.from_pandas(df)
        pq.write_table(table, buffer, compression='gzip')
        buffer.seek(0)
        blob = self.bucket.blob(gcs_file_path)
        blob.upload_from_file(buffer, content_type='application/octet-stream')
    
    def read_parquet_gcs(self, gcs_file_path):
        blob = self.bucket.blob(gcs_file_path)
        with blob.open("rb") as f:
            table = pq.read_table(f)
        return table.to_pandas()

    def read_partitioned_parquet_gcs(self, parquet_path):
        import dask.dataframe as dd
        import gcsfs
        # Create a GCS filesystem object
        fs = gcsfs.GCSFileSystem()

        # Construct the full GCS path
        gcs_path = f'gs://{self.bucket_name}/{parquet_path}'

        # Read the partitioned Parquet files into a Dask DataFrame
        ddf = dd.read_parquet(gcs_path, filesystem=fs)
        
        return ddf

    def send_dask_dataframe_to_gcs(self, ddf, output_parquet_path, partition_cols=None):
        import dask.dataframe as dd
        import gcsfs
        # Create a GCS filesystem object
        fs = gcsfs.GCSFileSystem()

        # Construct the full GCS path
        gcs_path = f'gs://{self.bucket_name}/{output_parquet_path}'

        # Write the Dask DataFrame to Parquet files in GCS
        ddf.to_parquet(gcs_path, filesystem=fs, write_index=False, partition_on=partition_cols)