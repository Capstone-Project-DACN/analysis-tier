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

# Download JARs directly
RUN mkdir -p /app/jars && \
    wget -O /app/jars/aws-java-sdk-bundle-1.11.901.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar && \
    wget -O /app/jars/hadoop-aws-3.3.1.jar https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar && \
    wget -O /app/jars/wildfly-openssl-1.0.7.Final.jar https://repo1.maven.org/maven2/org/wildfly/openssl/wildfly-openssl/1.0.7.Final/wildfly-openssl-1.0.7.Final.jar

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

EXPOSE 4040 4041

# Command to run the application
ENTRYPOINT ["/bin/sh", "-c", "export JAVA_HOME=$(dirname $(dirname $(readlink -f $(which java)))) && echo 'JAVA_HOME is set to: '$JAVA_HOME && exec python main.py"]
