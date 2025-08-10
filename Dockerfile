FROM python:3.11-slim

WORKDIR /app

COPY . /app

RUN pip install --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Make the script executable
RUN chmod +x /app/run_pipeline.sh

# Create required directories
# These directories are used for output, SQLite DBs, and Chroma persistent DBs
RUN mkdir -p /app/output /app/db/docs /app/db/vectordb


# Default command: run the pipeline script
CMD ["/app/run_pipeline.sh"]