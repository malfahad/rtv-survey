FROM python:3.9-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the application code
COPY . .

# Expose the port Flask runs on
EXPOSE 8081

# Set environment variables
ENV MINIO_ACCESS_KEY=rtv-test-user
ENV MINIO_SECRET_KEY=rtv-test-password

# Command to run the application
CMD ["python", "app.py"]
