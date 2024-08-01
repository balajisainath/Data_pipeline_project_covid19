# COVID-19 Data Processing Pipeline

## Table of Contents

- [Introduction](#introduction)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [License](#license)
- [Contact](#contact)

## Introduction

This project processes COVID-19 data sourced from Kaggle. It performs data validation using Great Expectations, preprocesses the data, and creates a data pipeline. Processed data is converted to Parquet format and stored in a database using DuckDB. Additionally, the project supports reading and writing data to AWS S3. Logging is implemented to facilitate debugging and tracking.

## Features

- Data validation with Great Expectations
- Data preprocessing and pipeline creation
- Conversion of data to Parquet format
- Integration with DuckDB for database storage
- AWS S3 support for reading and writing data
- Comprehensive logging for debugging
- Basic unit tests for critical methods

## Installation

### Prerequisites

Ensure you have Python installed. Install the required packages using `pip`.

#### Steps

- clone repository
```sh
git clone https://github.com/balajisainath/Data_pipeline_project_covid19.git
```

- Navigate to the project directory
```sh
cd Data_project
```

- pip install -r requirements.txt
```sh
pip install -r requirements.txt
```



## Usage
### Running the Project
- to run the main script
```sh
python src/__main__.py
```

### Reading and Writing Data from AWS S3
-Refer to data_reading.py for methods to read and write data from/to AWS S3 .

### Data Conversion
-Use data_conversion.py for methods to convert data to CSV and Parquet formats.

## Configuration

### Environment Variables
-Create a .env file in the project root directory to store your configuration variables.
```sh
ACCESS_KEY=your-access-key
ACCESS_SECRET_KEY=your-access-secret-key

```

## Project Structure
```sh
Data_Project
├───data
│   ├───db_files
│   ├───input
│   │   └───*.csv (input CSV files)
│   ├───intermediate
│   │   └─── (corrupted data files)
│   ├───output
│   │   └───*.parquet (processed data files)
│   └───test_output
├───logs
└───src
    ├───__main__.py
    └───other .py files
```

## License
This project is licensed under the MIT License.


## Contact
- Email: 
- GitHub: balaji sainath








