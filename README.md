# mutual_fund_recommendation_etl


## 🛠 Setup

Clone project
```git clone https://github.com/lijoabraham/mutual_fund_recommendation_etl.git```

### Build airflow Docker
```
$ cd mutual_fund_recommendation_etl/airflow/
$ docker build --rm -t mutual_fund_recommendation_etl:latest .
```
Change the image name in the below place of docker-compose.yaml file, if you have a different image name
```
airflow-webserver:
        image: mutual_fund_recommendation_etl:latest
```

### Launch containers
```
$ cd mutual_fund_recommendation_etl/
$ docker-compose -f docker-compose.yml up -d
```


## 👣 Additional steps
### Create a test user for Airflow
```
$ docker-compose run airflow-webserver airflow users create --role Admin --username admin \
  --email admin --firstname admin --lastname admin --password admin
```

### Edit connection from Airflow to Spark
- Go to Airflow UI > Admin > Edit connections
- Edit spark_default entry:
  - Connection Type: **Spark**
  - Host: **spark://spark**
  - Port: **7077**

### Check accesses
- Airflow: http://localhost:8080 (admin/admin)
- Spark Master: http://localhost:8081
- Apache superset - http://localhost:8088 (admin/admin)
- Jupyter Notebook - http://localhost:8888 ( follow along for token/password)

### For importing dump in MySQL
- Login to mysql docker container and import the ```dump-superset-latest``` SQL file 

### For running the commands manually
- Login to ```mutual_fund_recommendation_etl:latest``` container and run the following jobs
        - Scrap data 
                ```
                python src/app/jobs/mfscrapper.py
                ```
        - Spark job 
                  ```
                  spark-submit --master spark://spark:7077 --files /usr/local/spark/app/configs/scrapper.json --py-files /usr/local/spark/app/packages.zip --jars=/usr/local/spark/app/dependencies/mysql-connector-j-8.0.31.jar --name arrow-spark --verbose --queue root.default /usr/local/spark/app/jobs/etl_job.py
                  ```

### Reference links
https://zerodha.com/varsity/chapter/how-to-analyze-a-debt-mutual-fund/
https://www.crisil.com/content/dam/crisil/generic-images1/what-we-do/financial-products/mutual-fund-ranking/CRISIL_Mutual_Fund_Ranking_Methodology.pdf
