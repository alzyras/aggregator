FROM python:3.12-slim

# Python environment variables
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100

# Set your custom environment variable
ENV YOUR_ENV=aggregator

# Install dependencies
RUN apt-get update && apt-get install -y curl git && rm -rf /var/lib/apt/lists/*

# Set the working directory for the application
WORKDIR /app

# Copy the README file first (needed for metadata)
COPY README.md /app/

# Copy only the necessary files first (for better caching)
COPY pyproject.toml /app/
COPY requirements.txt /app/

# Install dependencies using pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application
COPY . /app/

# Install python-dotenv for .env file support
RUN pip install python-dotenv

# Set the PYTHONPATH environment variable
ENV PYTHONPATH=/app:$PYTHONPATH

# Set the working directory to ensure the app runs in the right path
WORKDIR /app

# Run the application
CMD ["python", "aggregator/run_all.py"]