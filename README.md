# fabio-zioli-subito-dataengineer-challenge


# Weather Data Processing Pipeline

## 1. Project Objective
The goal of this project is to design, implement, and test a robust data processing pipeline using **Apache Spark**. The pipeline analyzes historical meteorological datasets to extract meaningful insights regarding weather patterns, statistics, and seasonal temperature shifts across various global cities.

## 2. Dataset Overview
The analysis is based on five historical CSV datasets:
* `city_attributes.csv`: Static attributes (Latitude, Longitude) for each city.
* `humidity.csv`: Hourly humidity percentage (%) readings.
* `pressure.csv`: Hourly atmospheric pressure (hPa) readings.
* `temperature.csv`: Hourly temperature (Kelvin) readings.
* `weather_description.csv`: Hourly weather condition descriptions (e.g., "sky is clear").

### Data Constraints:
* **Timezones:** Primary data is in **UTC**. Conversion to local timezones is required for specific city-level analysis.
* **Data Quality:** The pipeline must handle null values, malformed timestamps, and missing records across different files.

## 3. Implemented Analytics Tasks

### Task 1: Clear Weather Spring Analysis
Identifies cities that recorded at least **15 "clear weather" days per month** during the spring season for every available year.
* **Condition:** The requirement must be met for all three months of the spring season.
* **Logic:** A custom criterion was established to define a "clear day" based on the hourly `weather_description` data.

### Task 2: Global Weather Statistics
Calculates monthly and yearly descriptive statistics for each country:
* **Metrics:** Mean, Standard Deviation, Minimum, and Maximum.
* **Variables:** Temperature, Pressure, and Humidity.

### Task 3: Seasonal Temperature Range (Top 3 Cities)
Finds the top 3 cities per country with the highest temperature variation between "hot" and "cold" periods in 2017, specifically during the **12:00-15:00** local time window.
* **Hot Period:** June, July, August, September.
* **Cold Period:** January, February, March, April.
* **Comparison:** Includes a ranking comparison between 2017 and 2016 results.

## 4. Technical Stack
* **Engine:** Apache Spark 3.x
* **Language:** Python (PySpark)
* **Environment:** PyCharm / Local Spark Cluster
* **Dependency Management:** `requirements.txt` / `Pipfile`

## 5. Development Standards
* **Modularity:** Separation of concerns between Data Extraction (ETL), Transformation Logic, and Utilities.
* **Type Hinting:** Extensive use of Python Type Hints for improved maintainability.
* **Robustness:** Automated cleaning of column names (replacing spaces with underscores) and schema enforcement.
* **Testing:** Unit tests covering the core transformation logic.