# Project 6: Data Engineering Capstone

&nbsp;

## Introduction
The purpose of the data engineering capstone project is to give you a chance to combine what we've learned throughout the Udacity DEND program. We'll define the scope of the project and the data we'll be working with. We'll gather data from several different data sources; transform, combine, and summarize it; and create a clean database for others to analyze.

&nbsp;

## Scope
This project aims to be able to answers questions on US immigration such as what are the most popular cities for immigration, what is the gender distribution of the immigrants, what is the visa type distribution of the immigrants, what is the average age per immigrant and what is the average temperature per month per city. We extract data from 3 different sources, the I94 immigration dataset of 2016, city temperature data from Kaggle and US city demographic data from OpenSoft. We design 4 dimension tables: Cities, immigrants, monthly average city temperature and time, and 1 fact table: Immigration. We use Spark for ETL jobs and store the results in parquet for downstream analysis.

&nbsp;

## Data
The data are pulled from three different sources and are distributed across fact and dimension tables to be able to do analysis on US immigration using factors of city monthly average temperature, city demographics and seasonality.

### Sources:
1. I94 Immigration Data: comes from the U.S. National Tourism and Trade Office and contains various statistics on international visitor arrival in USA and comes from the US National Tourism and Trade Office. The dataset contains data from 2016. [link](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
2. World Temperature Data: comes from Kaggle and contains average weather temperatures by city. [link](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
3. U.S. City Demographic Data: comes from OpenSoft and contains information about the demographics of all US cities such as average age, male and female population. [link](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).

### Model:
The star schema was chosen as the data model to write simple queries by joing fact and dimension tables in order to analyze the data. While for processing the data, we've chosen Spark was since it is known for speed in handling large amount of data (with in-memory compute), scale easily with additional worker nodes, has the ability to digest different data formats (e.g. SAS, Parquet, CSV), and integrate nicely with cloud storages like S3 and Warehouse like Redshift.

The data update cycle is typically chosen on two criteria. One is the reporting cycle, the other is the availabilty of new data to be fed into the system. For example, if new batch of average temperature can be made available at monthly interval, we might settle for monthly data refreshing cycle. There are also considerations in terms of scaling existing solution. If the data was increased by 100x: We can consider spinning up larger instances of EC2s hosting Spark and/or additional Spark work nodes. With added capacity arising from either vertical scaling or horizontal scaling, we should be able to accelerate processing time. 

If the data populates a dashboard that must be updated on a daily basis by 7am every day: We can consider using Airflow to schedule and automate the data pipeline jobs. Built-in retry and monitoring mechanism can enable us to meet user requirement. Moreover, if the database needed to be accessed by 100+ people: We can consider hosting our solution in production scale data warehouse in the cloud, with larger capacity to serve more users, and workload management to ensure equitable usage of resources across users.

#### Schema Tables:
*Staging Tables*

    staging_i94_df
        id
        date
        city_code
        state_code
        age
        gender
        visa_type
        count

    staging_temp_df
        year
        month
        city_code
        city_name
        avg_temperature
        lat
        long

    staging_demo_df
        city_code
        state_code
        city_name
        median_age
        pct_male_pop
        pct_female_pop
        pct_veterans
        pct_foreign_born
        pct_native_american
        pct_asian
        pct_black
        pct_hispanic_or_latino
        pct_white
        total_pop


*Dimension Tables*

    immigrant_df
        id
        gender
        age
        visa_type

    city_df
        city_code
        state_code
        city_name
        median_age
        pct_male_pop
        pct_female_pop
        pct_veterans
        pct_foreign_born
        pct_native_american
        pct_asian
        pct_black
        pct_hispanic_or_latino
        pct_white
        total_pop
        lat
        long

    monthly_city_temp_df
        city_code
        year
        month
        avg_temperature

    time_df
        date
        dayofweek
        weekofyear
        month

*Fact Table*

    immigration_df
        id
        state_code
        city_code
        date
        count

#### Pipeline:
Steps necessary to pipeline the data into the chosen data schema:
1. Clean the data on nulls, data types, duplicates, etc
2. Load staging tables for staging_i94_df, staging_temp_df and staging_demo_df
3. Create dimension tables for immigrant_df, city_df, monthly_city_temp_df and time_df
4. Create fact table immigration_df with information on immigration count, mapping id in immigrant_df, city_code in city_df and monthly_city_temp_df and date in time_df ensuring referential integrity
5. Save processed dimension and fact tables in parquet for downstream query

&nbsp;
