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

### The Three Sources:
1. I94 Immigration Data: comes from the U.S. National Tourism and Trade Office and contains various statistics on international visitor arrival in USA and comes from the US National Tourism and Trade Office. The dataset contains data from 2016. [link](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
2. World Temperature Data: comes from Kaggle and contains average weather temperatures by city. [link](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data).
3. U.S. City Demographic Data: comes from OpenSoft and contains information about the demographics of all US cities such as average age, male and female population. [link](https://public.opendatasoft.com/explore/dataset/us-cities-demographics/export/).

&nbsp;
