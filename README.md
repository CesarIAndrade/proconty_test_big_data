## Proconty test Big Data
This project was created in order to solve a challenge provided by Proconty, this one consists in an exercise where you have to get a dataset from Kaggle.com, store it in a db, and migrate the stored data to another db

### Used resources
- dataset: https://www.kaggle.com/datasets/rohithmahadevan/canada-employment-trend-cycle-dataset-official
- first db: MySql
- second db: Sqlite

### How to run
1. go to https://www.kaggle.com/datasets/rohithmahadevan/canada-employment-trend-cycle-dataset-official and download the csv file, then rename it to "canada_employment_trend_cycle_dataset"
2. clone this repo
3. open index.py
4. go to "connect to mysql" section and modify (if needed) the credentials (if you don't have mysql installed I recommend to use "laragon" or "xampp" ("lampp" for unix))
5. cmd: python index.py

### Workflow

1. the process will open the downloaded file: "canada_employment_trend_cycle_dataset.csv"
2. some column renaming and column deleting steps will be executed
3. the data will be splitted into batches of 1k of rows each one
4. the first insertion process will be executed
5. the stored records will be loaded
6. those records will be stored into the anothe db