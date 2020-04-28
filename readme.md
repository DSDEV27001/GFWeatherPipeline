## **Introduction**

This is an ETL pipeline project that ingests monthly weather data CSV files. It cleans and validates the data from these files. Further, data is transformed and enriched before being written 
to a parquet file. This project is set up so that the resulting parquet file is then queried with Apache Drill using a custom SQL query.

### **Prerequisites**

1. Drill must be installed and running (see reference link (3) for more information)
2. A LocationIQ geo-coding account and the environment variable APIKey set to the secret API key provided (if you intend to run the geo-coding module)

### **Note**

The geo-coding module was used to help verify accuracy of the provided region and country using reverse geo-coding of the latitude and longitude but it is not necessary for production code.

### **Usage**

Ensure Apache Drill is running and that the correct paths are specified at the top of the `__main__.py` module  and then run this script to produce the parquet file from the given weather files.

To run the geo-coding add-on make sure you have met the prerequisites and generated the ForecastSiteCords.csv by inserting the wp.export_cords function into the main script after wp.validate_weather_data and running this script. Then run the geocoding.py script. Output is written to ForecastSiteAddresses.csv in the Data folder.

### **Outputs**

(1) A parquet file containing all weather data

(2) Answers to the below questions written to standard output
- Which date was the hottest day?
- What was the temperature on that day?
- In which region was the hottest day?

(3) Exceptions written to the file in the project route error.log

(4) [Optional] Reverse geo-coding information for Forecast site Locations

### **Assumptions**

- The code focus should be on the two existing files (the code is well structured code to allow refactoring if the need to process further monthly files is required) 
- Data files sizes are constant as they are limited by the number of UK weather stations
- Column units correspond to those in reference link (2) where there is an equivalent
- SignificantWeatherCode and Visibility correspond to Weather Type Codes and Visibility in reference link (1)
- The parquet query does not exclude average temperatures for a given day where there is not a full day of readings
- The hottest day is the day with the highest daily average temperature not the day with the highest reached temperature for any hour of the day
- '-99' in WindSpeed, ScreenTemperature and SignificantWeatherCode columns indicates that data is not available and thus equates to null
- Enrichment of this data is valuable for downstream analysis and modelling
- Exhaustive code coverage for testing is not required for this task
- Exhaustive error handling is not required for this task
- Some specified assumptions have been made in data ranges for validation

### **Data Ranges**

- Signed degrees for coordinates mean latitudes are [-90, 90], and longitudes are [-180, 80]
- ScreenTemperature is [-50, 50] degrees celsius
- WindSpeed and WindGust are considered valid if less than 255 mph as the highest ever recorded wind was 253 mph
- Air pressures are [870, 1085] hPa as these are the current recorded minimum and maximum at Earth surface level
- Visibility will not exceed 125,000 [0 - 125,000] 
- Text strings will not exceed length 50 in the original weather data files

### **Reference Links**

1. Met-office weather types and visibility code definitions https://www.metoffice.gov.uk/services/data/datapoint/code-definitions
2. Met-office hourly site-specific observations units https://www.metoffice.gov.uk/services/data/datapoint/uk-hourly-site-specific-observations
3. Instructions on how to install Drill in embedded mode https://drill.apache.org/docs/embedded-mode-prerequisites/

