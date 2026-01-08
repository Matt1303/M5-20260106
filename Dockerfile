# Project Dockerfile for Library System
FROM python:3.12-slim

# Keep Python output unbuffered
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1


WORKDIR /app

# Install minimal system packages needed by some Python wheels/builds
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install Python deps
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . /app

# Default command shows the cleaning script help; override to run other commands
CMD ["python", "library_data_cleaning.py", "--help"]
