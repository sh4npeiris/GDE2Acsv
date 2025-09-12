# Use a base image with Python installed
# CHANGED: from buster to bullseye
FROM python:3.9-slim-bullseye

# Set working directory
WORKDIR /app

# Copy requirements file and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Install PyInstaller
RUN pip install pyinstaller

# ADD THIS LINE: Install binutils package which provides objdump
RUN apt-get update && apt-get install -y binutils

# Copy your application source code and spec file
COPY . .

# Build the PyInstaller executable
# Note the ':' for --add-data, which is correct for Linux
RUN pyinstaller --onefile --name GDE2Acsv --add-data "config:config" --add-data "data/input:data/input" --add-data "data/output:data/output" --distpath bin --hidden-import=pandas --hidden-import=yaml --hidden-import=logging.config src/main.py

# Set entrypoint to run the built executable (optional, for testing the image)
# ENTRYPOINT ["/app/bin/GDE2Acsv"]

# docker build -t gde2acsv-linux-builder .
# docker run --rm -v "$(pwd)/linux_output:/output" gde2acsv-linux-builder cp /app/bin/GDE2Acsv /output/