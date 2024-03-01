# Use the specific Python base image
FROM python:3.10.12-slim-bullseye

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file to the container
COPY requirements.txt /app/

# Install any dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of your application's code
COPY . /app

# Command to run your application
CMD ["python3", "your-app-script.py"]
