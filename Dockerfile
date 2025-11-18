# PySpark 3.5.5 - Production Ready Container
FROM apache/spark:3.5.5-scala2.12-java17-python3-ubuntu

USER root

WORKDIR /app

ENV SPARK_HOME=/opt/spark
ENV SPARK_JARS_DIR=$SPARK_HOME/jars

# Basic environment variables
ENV MPLCONFIGDIR="/tmp/matplotlib" \
    CLASSPATH=$SPARK_JARS_DIR/* \
    PATH=$PATH:$SPARK_JARS_DIR

# Copy requirements first for better caching
COPY requirements.txt /app/requirements.txt

# Install pip and create virtual environment for Python packages
RUN apt-get update && apt-get install -y curl python3-pip python3-venv && \
    python3 -m venv /opt/venv && \
    /opt/venv/bin/pip install --upgrade pip && \
    /opt/venv/bin/pip install -r requirements.txt

# Add virtual environment to PATH
ENV PATH="/opt/venv/bin:$PATH"

# Copy application code
COPY . /app/

RUN chmod 777 /app

USER 185
ENV PYTHONPATH=/app
