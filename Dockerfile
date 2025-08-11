FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Copy only requirements first (layer caching)
COPY requirements.txt .

# Install Python dependencies
RUN pip install --upgrade pip \
 && pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . /app

# Create runtime directories for output, SQLite DBs, and Chroma DBs
RUN mkdir -p /app/output /app/db/docs /app/db/vectordb /app/config

# Default command: run unified pipeline
CMD ["python", "run_pipeline.py"]

