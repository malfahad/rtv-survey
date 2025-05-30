import pandas as pd
import logging
from minio import Minio
from io import StringIO

logger = logging.getLogger(__name__)

class DataExtractor:
    def __init__(self):
        # Initialize MinIO client
        self.minio_client = Minio(
            "minio:9000",
            access_key="rtv-test-user",
            secret_key="rtv-test-password",
            secure=False
        )
        self.bucket_name = "rtv-survey"

    def extract_survey_data(self) -> pd.DataFrame:
        """Load survey data from MinIO"""
        try:
            # Get list of CSV files in the bucket
            objects = self.minio_client.list_objects(self.bucket_name, recursive=True)
            csv_files = [obj.object_name for obj in objects if obj.object_name.endswith('.csv')]
            
            if not csv_files:
                raise FileNotFoundError("No CSV files found in MinIO bucket")
            
            # Load data from each CSV file
            dfs = []
            for file_name in csv_files:
                try:
                    # Get file content
                    data = self.minio_client.get_object(self.bucket_name, file_name)
                    # Read CSV data into DataFrame
                    df = pd.read_csv(StringIO(data.read().decode('utf-8')))
                    # Add source file column
                    df['source_file'] = file_name
                    dfs.append(df)
                    logger.info(f"Successfully loaded data from {file_name}")
                except Exception as e:
                    logger.error(f"Error loading {file_name}: {str(e)}")
                    continue
            
            if not dfs:
                raise ValueError("No data could be loaded from CSV files")
            
            # Combine all DataFrames
            combined_df = pd.concat(dfs, ignore_index=True)
            logger.info("Survey data loaded successfully from MinIO")
            return combined_df
            
        except Exception as e:
            logger.error(f"Error loading survey data: {str(e)}")
            raise
