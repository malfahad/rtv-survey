from flask import Flask, Blueprint, flash, render_template, request, url_for, redirect
from werkzeug.exceptions import abort
from datetime import datetime
from threading import Thread, Event
from typing import Optional
import logging
import pandas as pd

from extract import DataExtractor

app = Flask(__name__)
app.config['SECRET_KEY'] = 'rtv_test_secret_key_here'

bp = Blueprint('pipeline', __name__)

# Setup logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

file_handler = logging.FileHandler("pipeline.log")
file_handler.setFormatter(
    logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter('%(levelname)s - %(message)s')
)
logger.addHandler(console_handler)

class PipelineJob(DataExtractor):
    def __init__(self, id: str, status: str, message: str, start_time=None):
        super().__init__()
        self.id = id
        self.status = status
        self.message = message
        self.start_time = start_time or datetime.now()
        self.pause_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.thread: Optional[Thread] = None

    def start(self):
        """Start the pipeline in a background thread"""
        if self.thread and self.thread.is_alive():
            logger.warning(f"Pipeline {self.id} is already running")
            return

        self.thread = Thread(target=self.run_pipeline, daemon=True)
        self.thread.start()
        logger.info(f"Started pipeline thread {self.id}")

    def pause(self):
        """Pause the entire pipeline"""
        self.pause_time = datetime.now()
        super().pause()
        logger.info(f"Pipeline {self.id} paused")

    def resume(self):
        """Resume the entire pipeline"""
        self.status = "Running"
        self.message = "Pipeline resumed"
        self.pause_time = None
        super().resume()
        logger.info(f"Pipeline {self.id} resumed")

    def stop(self):
        """Stop the entire pipeline"""
        super().stop()
        logger.info(f"Pipeline {self.id} stopped")

    def run_pipeline(self):
        try:
            self.status = "Running"
            self.message = "Starting data processing..."
            
            # Load data
            self.message = "Loading survey data..."
            df = self.extract_survey_data()
            
            # Transform data
            self.message = "Transforming data..."
            transformed_data = self.transform_survey_data(df)
            
            # Load to MinIO
            self.message = "Loading to MinIO..."
            
            # Load both detailed and overall metrics
            self.wait_for_resume()
            self.load_to_minio(transformed_data['detailed_metrics'], survey_year='2021', object_name='detailed_metrics.csv')
            self.wait_for_resume()
            self.load_to_minio(transformed_data['overall_metrics'], survey_year='2021', object_name='overall_metrics.csv')
            
            # Load to database
            self.message = "Loading to database..."
            self.wait_for_resume()
            self.load_to_database(transformed_data['detailed_metrics'], survey_year='2021')
            
            self.status = "Completed"
            self.message = "Pipeline completed successfully"
            self.end_time = datetime.now()
            logger.info(f"Pipeline {self.id} completed successfully")
        except KeyboardInterrupt:
            logger.info(f"Pipeline {self.id} stopped by user")
            self.status = "Stopped"
            self.message = "Pipeline stopped by user"
        except Exception as e:
            logger.error(f"Pipeline failed: {str(e)}")
            self.status = "Failed"
            self.message = f"Pipeline failed: {str(e)}"
            logger.error(f"Pipeline {self.id} failed: {str(e)}")
        finally:
            super().stop()

    def cancel(self):
        """Cancel the pipeline"""
        self.status = "Cancelled"
        self.message = "Pipeline cancelled"
        self.end_time = datetime.now()
        
        # Set stop event to signal thread to stop
        self.stop_event.set()
        
        # Wait for thread to finish
        if self.thread and self.thread.is_alive():
            self.thread.join()
            self.thread = None
        
        logger.info(f"Pipeline {self.id} cancelled")

    @property
    def duration(self):
        """Return the duration of the job in seconds"""
        if self.status == 'Running':
            return (datetime.now() - self.start_time).total_seconds()
        elif self.status == 'Paused':
            if self.pause_time:
                return (self.pause_time - self.start_time).total_seconds()
            return 0
        else:  # Completed, Failed, Cancelled
            if self.end_time:
                return (self.end_time - self.start_time).total_seconds()
            return 0

class PipelineManager:
    def __init__(self):
        self.jobs = {}
        
    def create_job(self):
        job_id = len(self.jobs) + 1
        job = PipelineJob(
            id=job_id,
            status="Running",
            message="Starting pipeline..."
        )
        job.start()
        self.jobs[job_id] = job
        return job

    def get_job(self, job_id):
        return self.jobs.get(job_id)

    def update_job_status(self, job_id, status, message):
        job = self.get_job(job_id)
        if job:
            job.status = status
            job.message = message
            return True
        return False

pipeline_manager = PipelineManager()

@bp.route('/')
def index():
    return render_template(
        'index.html',
        jobs=reversed(pipeline_manager.jobs.values())
    )

@bp.route('/new', methods=['POST'])
def new_job():
    if any(job.status in ('Running', 'Paused') for job in pipeline_manager.jobs.values()):
        flash('Cannot start a new pipeline while another is running or paused')
        return redirect('/')

    job = pipeline_manager.create_job()
    flash(f'Started new pipeline job #{job.id}')
    return redirect('/')

@bp.route('/<int:job_id>/pause', methods=['POST'])
def pause_job(job_id):
    job = pipeline_manager.get_job(job_id)
    job.pause()
    if pipeline_manager.update_job_status(
        job_id, 
        "Paused", 
        "Pipeline paused"
    ):
        flash('Job paused successfully')
    else:
        flash('Error pausing job')
    return redirect('/')

@bp.route('/<int:job_id>/resume', methods=['POST'])
def resume_job(job_id):
    job = pipeline_manager.get_job(job_id)
    job.resume()
    if pipeline_manager.update_job_status(
        job_id, 
        "Running", 
        "Pipeline resumed"
    ):
        flash('Job resumed successfully')
    else:
        flash('Error resuming job')
    return redirect('/')

@bp.route('/<int:job_id>/cancel', methods=['POST'])
def cancel_job(job_id):
    job = pipeline_manager.get_job(job_id)
    job.cancel()
    if pipeline_manager.update_job_status(
        job_id, 
        "Cancelled", 
        "Pipeline cancelled"
    ):
        flash('Job cancelled successfully')
    else:
        flash('Error cancelling job')
    return redirect('/')

app.register_blueprint(bp)

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8081)
