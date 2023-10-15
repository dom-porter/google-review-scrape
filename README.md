# Description

This is a threaded script to scrape information from Goolge maps about specific businesses.

# Input

The script will read in the targeted business from a .csv file in the following format:

[Your Ref],[Business name + partial address]

for example

us_675,Empire Outlets US NY 10301


The [Your Ref] is for you so you can process the data as needed once you have the initial data set.


# Output

The script will output 3 .csv files. The [output prefix] is provided when the script is run.<br/>Reviews are limited to the most recent 1000.



| filename                          | contents                                                                                        |
|-----------------------------------|-------------------------------------------------------------------------------------------------|
| [output prefix]_details.csv       | business_ref,<br/>business_name,<br/>address,avg_rating,<br/>total_reviews,</br>service_options |
| [output prefix]_popular_times.csv | business_ref,<br/>percent_busy,<br/>hour_no,<br/>each_hour,<br/>day_of_week                     |
| [output prefix]_reviews.csv       | business_ref,<br/>reviewer_name,<br/>rating,reviewed_dt,<br/>review                             |





# Python Version

```
3.10
```

# Install

```
rename .envtemplate to .env

pip install -r requirements.txt
```

# Configuration

| .env Variable    | Description                           | Default           |
|------------------|:--------------------------------------|:------------------|
| G_MAPS_LOG_NAME  | The filename of the log file          | google-scrape.log |
| G_MAPS_LOG_SIZE  | THe max size of the log file in bytes | 150000            |
| G_MAPS_LOG_COUNT | The number of log files to keep       | 2                 |
| G_MAPS_LOG_DEBUG | Enable debug logging                  | false             |
| G_MAP_THREADS    | The number of threads to use          | 2                 |


# Running


```
python3 main.py input.csv 01_01_2023
```
## License
Apache License Version 2.0