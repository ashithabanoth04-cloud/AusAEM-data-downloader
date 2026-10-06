# Use an official lightweight Python image
FROM python:3.9-slim

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install all the python packages including pandas, boto3, etc.
RUN pip install --no-cache-dir -r requirements.txt

# Copy your Python code (ausem.py) into the container
COPY ausem.py .

# Command to run your Python script
CMD ["python", "ausem.py"]
