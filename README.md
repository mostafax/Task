# Comprehensive Sales Data Pipeline

This repository contains the implementation of a comprehensive sales data pipeline for a retail company. The pipeline integrates sales data with user information and weather data, performs necessary transformations and aggregations, and stores the final dataset in a relational database for further analysis.

## Table of Contents
- [Project Structure](#project-structure)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
  - [Usage](#usage)
- [Data Transformation](#data-transformation)
  - [Sales Data](#sales-data)
  - [User Data](#user-data)
  - [Weather Data](#weather-data)
- [Data Manipulation and Aggregations](#data-manipulation-and-aggregations)
- [Data Storage](#data-storage)
- [Dockerization](#dockerization)

# Project Structure
```plaintext
/task/
|-- src/
|   |-- main.py            # Main script to run the data pipeline
|   |-- extraction.py      # Modules to handle API requests
|   |-- transformation.py      # Modules for data transformation
|   |-- database_manager.py      # Modules for database operations
|   `-- db/                # Folder for database file 
|-- config/
|   |-- config.json            # All project configuration
|-- outputs/             # All project outputs
|-- Dockerfile             # Dockerfile for creating a containerized version
|-- requirements.txt       # The dependencies for the project
`-- README.md              # Documentation for the project
```

# Getting Started
## Prerequisites
- you need to create folder data inside data-pipeline/ containing sales data csv.

- You will also need to create API key for OpenWeatherMap.

## Installation

### Using Python

1. Ensure that Python version 3.10.12 is installed on your system. You can download it from the [official Python website](https://www.python.org/downloads/).
2. Install pip, Python's package installer, if it's not already installed.
3. Navigate to the root directory of the project and install the required dependencies with the following command:

```bash
pip install -r requirements.txt
```

### Using Docker

Alternatively, if you prefer to use Docker, build the Docker image with the following command:

```bash
docker build -t sales-data-pipeline .
```

This command creates a Docker image named sales-data-pipeline, encapsulating all the necessary dependencies.


## Usage

- After installation, you can run the data pipeline using one of two methods:
- Create outputs file to store all output on the same level as /task/


### Running Natively

```bash
cd src

python3 main.py
```

### Running in Docker


```bash
docker run -v /home/mostafa/workspace/task/outputs:/app/outputs/ -it docker.io/library/sales-data-pipeline
```


## Data Storage

![DB Design](src/db/db_desgin.png "DB Design")