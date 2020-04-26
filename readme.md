## **Introduction**

This is an ETL pipeline project that ingests monthly weather data CSV files. It cleans and validates the data from these files. Further, data is transformed and enriched before being written 
to a parquet file. This project is set up so that the resulting parquet file is then queried with Apache Drill using a custom SQL query.

### **Requirements**

Drill must be installed and running (see reference link (3) for more information)

### **Outputs**

(1) A parquet file containing all weather data

(2) Answers to the below questions written to standard output
- Which date was the hottest day?
- What was the temperature on that day?
- In which region was the hottest day?

(3) Exceptions written to a log file 

### **Assumptions**

- If further weather data was to be provided in monthly files the code could be adapted to check for new files in a specified folder and to append data to an existing parquet
- '-99' in WindSpeed, ScreenTemperature and SignificantWeatherCode columns indicates that data is not available and thus equates to null
- Data is UK only
- Data files sizes are constant as they are limited by the number of weather stations in the UK
- An ETL tool such as Luigi or similar, or the implementation of an AirFlow pipeline would be excessive given the size of datafiles
- Column units correspond to those in reference link (2) where there is an equivalent
- SignificantWeatherCode and Visibility correspond to Weather Type Codes and Visibility in reference link (1)
- The parquet query does not exclude average temperatures for a given day where there is not a full day of readings
- The hottest day is the day with the highest daily average temperature not the day with the highest reached temperature for any hour of the day

### **Data Ranges**

- Latitudes range from -90 to 90, and longitudes range from -180 to 80 as they are signed degrees
- ScreenTemperature is between -50 and 50 degrees celsius
- WindSpeed and WindGust wont exceed 255 mph (the highest ever recorded was 253 mph)
- Air pressure will be between 870 hPa to 1085 hPa as these are the current recorded minimum and maximum at surface level

### **Reference Links**

1. Met-office weather types and visibility code definitions https://www.metoffice.gov.uk/services/data/datapoint/code-definitions
2. Met-office hourly site-specific observations units https://www.metoffice.gov.uk/services/data/datapoint/uk-hourly-site-specific-observations
3. Instructions on how to install Drill in embedded mode https://drill.apache.org/docs/embedded-mode-prerequisites/