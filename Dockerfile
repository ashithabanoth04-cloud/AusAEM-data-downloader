# Use an official lightweight Python image
FROM python:3.9-slim

# Set the working directory
WORKDIR /app

# Copy requirements
COPY requirements.txt .

# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application files
COPY ausem.py .
COPY worker.py .
COPY s3_uploader.py .

# Start the worker
CMD ["python", "worker.py"]
