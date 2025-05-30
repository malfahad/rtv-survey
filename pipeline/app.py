from flask import Flask, Blueprint, flash, render_template, request, url_for, redirect
from werkzeug.exceptions import abort
from datetime import datetime
from threading import Thread, Event
from typing import Optional
import logging

from extract import DataExtractor
from transform import DataTransformer
from load import DataLoader

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

class PipelineJob:
    def __init__(self, id, status, message, start_time=None):
        self.id = id
        self.status = status
        self.message = message
        self.start_time = start_time or datetime.now()
        self.thread: Optional[Thread] = None
        self.stop_event = Event()

    def start(self):
        """Start the pipeline in a background thread"""
        if self.thread and self.thread.is_alive():
            raise RuntimeError("Job is already running")

        self.thread = Thread(target=self.run_pipeline)
        self.thread.daemon = True
        self.thread.start()
        logger.info(f"Started pipeline thread {self.id}")

    def run_pipeline(self):
        try:
            self.status = "Running"
            self.message = "Starting data processing..."
            
            # Load data
            self.message = "Loading survey data..."
            df = DataExtractor().extract_survey_data()
            
            # Transform data
            self.message = "Transforming data..."
            transformed_data = DataTransformer().transform_survey_data(df)
            
            # Load to MinIO
            self.message = "Loading to MinIO..."
            loader = DataLoader()
            
            # Load both detailed and overall metrics
            loader.load_to_minio(transformed_data['detailed_metrics'], survey_year='2021', object_name='detailed_metrics.csv')
            loader.load_to_minio(transformed_data['overall_metrics'], survey_year='2021', object_name='overall_metrics.csv')
            
            # Load to database
            self.message = "Loading to database..."
            loader.load_to_database(transformed_data['detailed_metrics'], survey_year='2021')
            
            self.status = "Completed"
            self.message = "Pipeline completed successfully"
            logger.info(f"Pipeline {self.id} completed successfully")
        except Exception as e:
            self.status = "Failed"
            self.message = f"Pipeline failed: {str(e)}"
            logger.error(f"Pipeline {self.id} failed: {str(e)}")
        finally:
            self.stop_event.set()

    def pause(self):
        self.status = "Paused"
        self.message = "Pipeline paused"
        self.stop_event.set()

    def resume(self):
        self.status = "Running"
        self.message = "Pipeline resumed"
        self.stop_event.clear()

    def cancel(self):
        self.status = "Cancelled"
        self.message = "Pipeline cancelled"
        self.stop_event.set()

    @property
    def duration(self):
        if self.start_time:
            return datetime.now() - self.start_time
        return None

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
        jobs=pipeline_manager.jobs.values()
    )

@bp.route('/new', methods=['POST'])
def new_job():
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
