FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=""

# Install dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jdk \
    wget \
    procps \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Create directories for logs and checkpoints
RUN mkdir -p /app/logs /tmp/checkpoint/ward_data /tmp/checkpoint/household_data

# Set environment variables for Spark
ENV PYSPARK_PYTHON=/usr/local/bin/python \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python


# Copy application code
COPY finish/*.py .
COPY jars /app/jars


# Command to run the application
ENTRYPOINT ["/bin/sh", "-c", "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && echo 'JAVA_HOME is set to: '$JAVA_HOME && exec python main.py"]


