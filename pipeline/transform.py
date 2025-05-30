import pandas as pd
from typing import Optional
from datetime import datetime
import logging
from load import DataLoader

logger = logging.getLogger(__name__)

class DataTransformer(DataLoader):
    def __init__(self):
        super().__init__()

    def transform_survey_data(self, df: pd.DataFrame) -> dict:
        """Transform survey data into meaningful metrics"""
        try:
            # Create a copy to avoid fragmentation warnings
            df = df.copy()
            
            # Convert date columns to datetime if they exist
            self.wait_for_resume()
            datetime_cols = ['SubmissionDate', 'starttime', 'endtime']
            for col in datetime_cols:
                self.wait_for_resume()
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col])
            
            # Calculate survey duration if datetime columns exist
            self.wait_for_resume()
            if 'starttime' in df.columns and 'endtime' in df.columns:
                df['survey_duration'] = (df['endtime'] - df['starttime']).dt.total_seconds() / 60
            
            # Get available grouping columns
            self.wait_for_resume()
            grouping_cols = ['district', 'Quartile', 'source_file']
            available_cols = [col for col in grouping_cols if col in df.columns]
            
            # Define metrics to calculate
            metrics = {
                'household': {
                    'hhid_2': 'nunique',
                    'hh_size': ['mean', 'median'],
                    'hhh_age': ['mean', 'median'],
                    'hhh_sex': lambda x: x.value_counts().idxmax() if len(x.value_counts()) > 0 else None,
                    'survey_duration': ['mean', 'median'] if 'survey_duration' in df.columns else None,
                    'GPS-Accuracy': 'mean' if 'GPS-Accuracy' in df.columns else None
                },
                'education': {
                    'hhh_educ_level': lambda x: x.value_counts().idxmax() if len(x.value_counts()) > 0 else None,
                    'hhh_read_write': 'mean' if 'hhh_read_write' in df.columns else None,
                    'age_6_12_Attend_Sch_1': 'mean' if 'age_6_12_Attend_Sch_1' in df.columns else None,
                    'age_13_18_Attend_Sch_1': 'mean' if 'age_13_18_Attend_Sch_1' in df.columns else None
                },
                'assets': {
                    'Number_Radios': ['mean', 'sum'] if 'Number_Radios' in df.columns else None,
                    'Number_Mobile_Phones': ['mean', 'sum'] if 'Number_Mobile_Phones' in df.columns else None,
                    'assets_reported_total': ['mean', 'sum'] if 'assets_reported_total' in df.columns else None
                }
            }
            
            # Remove None values from metrics
            self.wait_for_resume()
            metrics = {k: {k2: v2 for k2, v2 in v.items() if v2 is not None} 
                      for k, v in metrics.items()}
            
            # Calculate metrics
            detailed_metrics = pd.DataFrame()
            overall_metrics = pd.DataFrame()
            
            if available_cols:
                # Calculate detailed metrics
                detailed_metrics = df.groupby(available_cols).agg({
                    **metrics['household'],
                    **metrics['education'],
                    **metrics['assets']
                }).reset_index()
                
                # Flatten multi-level columns
                detailed_metrics.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col 
                                         for col in detailed_metrics.columns.values]
                
                self.wait_for_resume()
                # Calculate overall metrics
                overall_metrics = df.groupby(['district', 'Quartile']).agg({
                    **metrics['household'],
                    **metrics['education'],
                    **metrics['assets']
                }).reset_index()
                
                # Flatten overall metrics columns
                self.wait_for_resume()
                overall_metrics.columns = ['_'.join(col).strip() if isinstance(col, tuple) else col 
                                        for col in overall_metrics.columns.values]
            
            logger.info("Survey data transformed successfully")
            return {
                'detailed_metrics': detailed_metrics,
                'overall_metrics': overall_metrics
            }
        except Exception as e:
            logger.error(f"Error transforming survey data: {str(e)}")
            raise
