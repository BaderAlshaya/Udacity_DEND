# Project 2: Data Modeling with Apache Cassandra

&nbsp;

## Introduction
A startup called Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. The analytics team is particularly interested in understanding what songs users are listening to. Currently, they don't have an easy way to query the data gathered from their app. 

The *user activity* data is in a directory of CSV files. Therefore, they'd like us to create an Apache Cassandra database which can create queries on song play data to answer the questions. We decided to model the event data by creating tables in Apache Cassandra to run queries. 

Note that a part of the ETL pipeline that transfers the data has been provided initially. The provided pipline transfers the event data from a set of CSV files within a directory by creating a streamlined CSV file to model and insert data into Apache Cassandra tables.

&nbsp;

## Project Description
**Requirements:**
- We collect the data for user activity from a music streaming app called **Sparkify** in `CSV` file format.
- We creat a **Non-Relational (NoSQL) Database** to store the data using `Apache Cassandra` tables. 
- We process the `event_datafile_new.csv` dataset to create a denormalized dataset.
- We use the part of the **ETL Pipeline** being provided to load the date into tables that we create in `Apache Cassandra`.
- We create the database and run queries through `Project_1B_ Project_Template.ipynb`
- We finally test the database by comparing the given expected results with our own generated results.

&nbsp;

**Files:**
- (Jupyter Notebook) **`Project_1B_ Project_Template.ipynb`**: Analyse dataset before loading it to the database.


**Data:**
&nbsp;

1. Event Dataset:

Each event data is in `CSV` file format and contains metadata about a song and the artist of that song. The files are partitioned by date.
&nbsp;

**For instance:** 

> `2018-11-01-events.csv`
```
|artist                    |auth     |firstName|gender|itemInSession|lastName|length   |level|location                                    |method|page    |registration|sessionId|song                                            |status|ts         |userId|
|--------------------------|---------|---------|------|-------------|--------|---------|-----|--------------------------------------------|------|--------|------------|---------|------------------------------------------------|------|-----------|------|
|                          |Logged In|Walter   |M     |0            |Frye    |         |free |San Francisco-Oakland-Hayward, CA           |GET   |Home    |1.54092E+12 |38       |                                                |200   |1.54111E+12|39    |
|                          |Logged In|Kaylee   |F     |0            |Summers |         |free |Phoenix-Mesa-Scottsdale, AZ                 |GET   |Home    |1.54034E+12 |139      |                                                |200   |1.54111E+12|8     |
|Des'ree                   |Logged In|Kaylee   |F     |1            |Summers |246.30812|free |Phoenix-Mesa-Scottsdale, AZ                 |PUT   |NextSong|1.54034E+12 |139      |You Gotta Be                                    |200   |1.54111E+12|8     |
|                          |Logged In|Kaylee   |F     |2            |Summers |         |free |Phoenix-Mesa-Scottsdale, AZ                 |GET   |Upgrade |1.54034E+12 |139      |                                                |200   |1.54111E+12|8     |
|Mr Oizo                   |Logged In|Kaylee   |F     |3            |Summers |144.03873|free |Phoenix-Mesa-Scottsdale, AZ                 |PUT   |NextSong|1.54034E+12 |139      |Flat 55                                         |200   |1.54111E+12|8     |
|Tamba Trio                |Logged In|Kaylee   |F     |4            |Summers |177.18812|free |Phoenix-Mesa-Scottsdale, AZ                 |PUT   |NextSong|1.54034E+12 |139      |Quem Quiser Encontrar O Amor                    |200   |1.54111E+12|8     |
|The Mars Volta            |Logged In|Kaylee   |F     |5            |Summers |380.42077|free |Phoenix-Mesa-Scottsdale, AZ                 |PUT   |NextSong|1.54034E+12 |139      |Eriatarka                                       |200   |1.54111E+12|8     |
|Infected Mushroom         |Logged In|Kaylee   |F     |6            |Summers |440.2673 |free |Phoenix-Mesa-Scottsdale, AZ                 |PUT   |NextSong|1.54034E+12 |139      |Becoming Insane                                 |200   |1.54111E+12|8     |
|Blue October / Imogen Heap|Logged In|Kaylee   |F     |7            |Summers |241.3971 |free |Phoenix-Mesa-Scottsdale, AZ                 |PUT   |NextSong|1.54034E+12 |139      |Congratulations                                 |200   |1.54111E+12|8     |
|Girl Talk                 |Logged In|Kaylee   |F     |8            |Summers |160.15628|free |Phoenix-Mesa-Scottsdale, AZ                 |PUT   |NextSong|1.54034E+12 |139      |Once again                                      |200   |1.54111E+12|8     |
|Black Eyed Peas           |Logged In|Sylvie   |F     |0            |Cruz    |214.93506|free |Washington-Arlington-Alexandria, DC-VA-MD-WV|PUT   |NextSong|1.54027E+12 |9        |Pump It                                         |200   |1.54111E+12|10    |
|                          |Logged In|Ryan     |M     |0            |Smith   |         |free |San Jose-Sunnyvale-Santa Clara, CA          |GET   |Home    |1.54102E+12 |169      |                                                |200   |1.54111E+12|26    |
|Fall Out Boy              |Logged In|Ryan     |M     |1            |Smith   |200.72444|free |San Jose-Sunnyvale-Santa Clara, CA          |PUT   |NextSong|1.54102E+12 |169      |Nobody Puts Baby In The Corner                  |200   |1.54111E+12|26    |
|M.I.A.                    |Logged In|Ryan     |M     |2            |Smith   |233.7171 |free |San Jose-Sunnyvale-Santa Clara, CA          |PUT   |NextSong|1.54102E+12 |169      |Mango Pickle Down River (With The Wilcannia Mob)|200   |1.54111E+12|26    |
|Survivor                  |Logged In|Jayden   |M     |0            |Fox     |245.36771|free |New Orleans-Metairie, LA                    |PUT   |NextSong|1.54103E+12 |100      |Eye Of The Tiger                                |200   |1.54111E+12|101   |
```

&nbsp;

## Build
To build the project (Mac OS):
- Install Jupyter Notebook (using pip or conda).

&nbsp;

## Run
Follow the steps below to compile and run the project:
- Open the following file with Jupyter Notebook `Project_1B_ Project_Template.ipynb`

&nbsp;

## Test Result
To test the data modeling compare the generated results with a screenshot of expected results found in the following directory:
- `./assets/images/image_event_datafile_new.jpg`

&nbsp;

![Expected Result](https://github.com/BaderAlshaya/Udacity_DEND/blob/master/p2/assets/images/image_event_datafile_new.jpg?raw=true)

&nbsp;
