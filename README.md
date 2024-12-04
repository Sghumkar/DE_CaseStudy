
# **Sensor Data Processing Pipeline**

## **Overview**
This pipeline processes and analyzes sensor data stored in a PostgreSQL database. Aggregations such as minimum, maximum, mean, and standard deviation are calculated for key measurements.

---

## **Dataset Information**

- **Dataset Link**:Dataset is from Kaggle [Beach Weather Stations Dataset](https://www.kaggle.com/datasets/sanjanchaudhari/beach-weather-stations)  
- **Database**: PostgreSQL  

### **Dataset Description**  
The dataset contains **18 columns**, but this pipeline uses the following key columns:  

- **Station Name**: Represents the data source.  
- **Measurement Timestamp**: Timestamp of the measurement.  
- **Air Temperature**: Temperature recorded by the sensor.  
- **Humidity**: Humidity level recorded by the sensor.  
- **Barometric Pressure**: Pressure recorded by the sensor.  
- **Measurement ID**: A unique identifier created by combining the `Station Name` and `Measurement Timestamp`.  

---

## **Prerequisites**

1. Ensure the PostgreSQL database is accessible and configured correctly.  
2. Update the `.env` file with the following:  
   ```plaintext
    DB_NAME=<your-database-name>
    DB_USER=<your-database-username>
    DB_PASSWORD=<your-database-password>
    DB_HOST=<your-database-host>
    DB_PORT=<your-database-port>
    DB_SCHEMA=<your-schema-name>
   ```

---

## **Setup and Execution Instructions**

### 1. **Create a Virtual Environment**
   - Open a terminal and navigate to your project directory.  
   - Run the following commands:  
     ```bash
     python3 -m venv env
     source env/bin/activate   # On macOS/Linux
     env\Scripts\activate      # On Windows
     ```

### 2. **Install Dependencies**
   - Ensure the `requirements.txt` file is in your project directory.  
   - Run:  
     ```bash
     pip install -r requirements.txt
     ```

### 3. **Run the Pipeline**
   - Ensure the `.env` file is correctly configured.  
   - Execute the pipeline using:  
     ```bash
     python main.py
     ```

### 4. **Deactivate the Virtual Environment**  
   - When done, deactivate the environment using:  
     ```bash
     deactivate
     ```

---

## **Project Structure**

```
sensor-data-pipeline/
│
├── src/                         # Source code
│   ├── aggregation.py           # Data aggregation functions
│   ├── data_handler.py          # File processing pipeline
│   ├── data_processor.py        # Data transformation and processing
│   ├── file_utils.py            # File handling utilities
│   ├── logger.py                # Logging setup and management
│   ├── monitor.py               # Watchdog for monitoring new files
│   ├── validation.py            # Data validation functions
├── db/                          # Database related scripts
│   ├── db_utils.py              # Database operations
│   ├── retry_utils.py           # Retry logic for DB operations
├── config/                      # Configuration files
│   ├── settings.py              # Settings for the pipeline
├── main.py                      # Entry point for running the pipeline
├── requirements.txt             # Python dependencies
├── .env                         # Environment configuration file
└── README.md                    # Documentation
```

---

## **Contributors**
- **Your Name**  

For questions or feedback, please reach out to **[Your Email](mailto:swapnalighumkarw@gmail.com)**.

---
