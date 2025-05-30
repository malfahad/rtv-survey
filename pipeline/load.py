import pandas as pd
import sqlalchemy
from typing import Optional
from datetime import datetime
import logging
from minio import Minio
import os
import io
import threading
import time

logger = logging.getLogger(__name__)

class DataLoader:
    def __init__(self):
        self.stop_event = threading.Event()
        self.paused_event = threading.Event()
        self.progress = None
        self.db_url = "postgresql://rtv-test-user:rtv-test-password@postgres:5432/rtv_survey"
        self.engine = sqlalchemy.create_engine(self.db_url)
        
        # Initialize MinIO client
        self.minio_client = Minio(
            "minio:9000",
            access_key=os.getenv('MINIO_ACCESS_KEY'),
            secret_key=os.getenv('MINIO_SECRET_KEY'),
            secure=False
        )
        self.bucket_name = "rtv-survey-results"

    def pause(self):
        """Pause the loading process"""
        self.paused_event.set()
        logger.info("Data loading paused")

    def resume(self):
        """Resume the loading process"""
        self.paused_event.clear()
        logger.info("Data loading resumed")

    def stop(self):
        """Stop the loading process"""
        self.stop_event.set()
        logger.info("Data loading stopped")

    def wait_for_resume(self):
        """Wait until paused event is cleared or stop event is set"""
        while self.paused_event.is_set() and not self.stop_event.is_set():
            time.sleep(1)
            if self.stop_event.is_set():
                raise KeyboardInterrupt("Loading stopped by user")

    def load_to_database(self, df: pd.DataFrame, survey_year: str):
        """Load transformed data to database"""
        try:
            self.wait_for_resume()
            
            # Start a transaction
            with self.engine.begin() as connection:
                # Truncate the table first
                connection.execute(sqlalchemy.text("""
                    TRUNCATE TABLE metrics.survey_summary;
                """))

                # Get existing columns from database
                result = connection.execute(sqlalchemy.text("""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_schema = 'metrics' 
                    AND table_name = 'survey_summary'
                """))
                db_columns = [row[0] for row in result]

                # Filter DataFrame to only include columns that exist in database
                df_filtered = df[[col for col in df.columns if col in db_columns]]

                # Load filtered data to database
                df_filtered.to_sql('survey_summary', connection, schema='metrics', 
                                 if_exists='append', index=False)
            
            logger.info("Data loaded to database successfully")
        except Exception as e:
            logger.error(f"Error loading data to database: {str(e)}")
            raise

    def load_to_minio(self, df: pd.DataFrame, survey_year: str, object_name: str = None):
        """Load data to MinIO with optional custom object name"""
        try:
            # Convert DataFrame to CSV
            csv_data = df.to_csv(index=False)
            
            # Generate object name if not provided
            if not object_name:
                object_name = f"survey_{survey_year}.csv"
            
            # Upload to MinIO
            self.minio_client.put_object(
                self.bucket_name,
                object_name,
                data=io.BytesIO(csv_data.encode('utf-8')),
                length=len(csv_data),
                content_type='application/csv'
            )
            logger.info(f"Data loaded to MinIO: {object_name}")
        except Exception as e:
            logger.error(f"Error loading data to MinIO: {str(e)}")
            raise
