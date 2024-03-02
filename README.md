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
- [Documentation](#documentation)
- [Dockerization](#dockerization)

## Project Structure
```plaintext
/data-pipeline/
|-- src/
|   |-- main.py            # Main script to run the data pipeline
|   |-- extraction.py      # Modules to handle API requests
|   |-- transforamtion.py      # Modules for data transformation
|   |-- database_manager.py/      # Modules for database operations
|   `-- db/                # Folder for database file 
|-- Dockerfile             # Dockerfile for creating a containerized version
|-- requirements.txt       # The dependencies for the project
`-- README.md              # Documentation for the project
```

## Getting Started
# Prerequisites
you need to create folder data inside data-pipeline/ containing sales data csv.
You will also need to create API key for OpenWeatherMap
# Installation

You will need to isntall  python (3.10.12), pip and the requrienatenr  file
or 
directly run docker 
