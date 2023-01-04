from pyspark.sql import Row
import os
import json
import datetime
import pandas as pd
from pyspark.sql.functions import udf, array, dense_rank, expr, when, col
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType
from dependencies import spark as spk
from dependencies import db

def calculate_growth(df):
        growth_3m = 1 if df[0] > df[7] else 0
        growth_6m = 1 if df[1] > df[8] else 0
        growth_1y = 1 if df[2] > df[9] else 0
        growth_3y = 1 if df[3] > df[10] else 0
        growth_5y = 1 if df[4] > df[11] else 0
        growth_7y = 1 if df[5] > df[12] else 0
        growth_10y = 1 if df[6] > df[13] else 0

        g_vals = [6, 5, 4, 3, 2, 1]
        trend = 0
        for i in g_vals:
            diff = df[i - 1] - df[i]
            if diff > 0:
                trend = trend + 1
            elif diff < 0:
                trend = trend - 1
            # print(diff, df[i], df[i - 1], trend)

        growth = 7 - (growth_3m + growth_6m + growth_1y + growth_3y + growth_5y + growth_7y + growth_10y)
        trend_growth = 7 - trend

        return growth + trend_growth

#  spark-submit --master spark://spark:7077 --files /usr/local/spark/app/configs/scrapper.json --py-files /usr/local/spark/app/packages.zip --jars=/usr/local/spark/app/dependencies/mysql-connector-j-8.0.31.jar --name arrow-spark --verbose --queue root.default /usr/local/spark/app/jobs/etl_job.py
class ETLJob(object):
    def __init__(self) -> None:
        self._config_data = self.get_config_data()
        self._db_session = db._connect_to_database(self._config_data)
        self.spark = None

    def main(self):
        """Main ETL script definition.
        :return: None
        """
        # start Spark application and get Spark session, logger and configs
        params = {'app_name': 'my_etl_job', 'files': ['/usr/local/spark/app/configs/scrapper.json'],'jars':['/usr/local/spark/app/dependencies/mysql-connector-j-8.0.31.jar']}
        # spark, log, config = spk.SparkConnection(params)
        spark_details = spk.SparkConnection(params)

        self.spark = spark_details.spark
        log = spark_details.log

        # log that main ETL job is starting
        log.warn('etl_job is up-and-running')

        # execute ETL pipeline
        data = self.extract_data()
        data_transformed = self.transform_data(data)
        self.load_data(data_transformed)
        # log the success and terminate Spark application
        log.warn('Spark job is finished')
        self.spark.stop()
        return None

    def get_config_data(self):
        config_file = '/usr/local/spark/app/configs/scrapper.json'
        with open(config_file, 'r') as f:
            config = json.load(f)
        return config
    
    def mysql_reader(self):
        reader = self.spark.read.format('jdbc').options(
            url='jdbc:mysql://host.docker.internal:3306/superset?useSSL=false',
            driver='com.mysql.cj.jdbc.Driver',
            user='root',
            password='root')
        return reader

    def save_data(self, df, table):
        df.write.format('jdbc').options(
            url='jdbc:mysql://host.docker.internal:3306/superset?useSSL=false',
            driver='com.mysql.cj.jdbc.Driver',
            user='root',
            dbtable=table,
            password='root').mode('overwrite').save()

    def read_data(self, table):
        reader =self.mysql_reader()
        source_df = reader.option("dbtable", table)
        return source_df.load()
        

    def extract_data(self):
        """Load data from Parquet file format.
        :param spark: Spark session object.
        :return: Spark DataFrame.
        """
        cur_date = '2023-01-03' #str(datetime.datetime.now().strftime("%Y-%m-%d"))
        mf_data_df = self.read_data('mf_data').filter(f"time_added == date'{cur_date}'")
        mf_risk_df = self.read_data('mf_risk').filter(f"time_added == date'{cur_date}'")
        mf_growth_df = self.read_data('mf_growth').filter(f"time_added == date'{cur_date}'")
        mf_credit_rating_df = self.read_data('mf_credit_rating').filter(f"time_added == date'{cur_date}'")
        mf_category_data_df = self.read_data('mf_category_data')
        mf_category_growth_df = self.read_data('mf_category_growth').filter(f"time_added == date'{cur_date}'")
        mf_category_risk_df = self.read_data('mf_category_risk').filter(f"time_added == date'{cur_date}'")
        
        mf_risk_df = mf_risk_df.select(['mf_id','time_modified']+[col(col_name).alias('mf_risk_' + col_name) for col_name in mf_risk_df.columns if col_name not in ['mf_id','time_modified']])
        mf_growth_df = mf_growth_df.select(['mf_id',col('time_added').alias('mf_grow_time_added'),'time_modified']+[col(col_name).alias('mf_growth_' + col_name) for col_name in mf_growth_df.columns if col_name not in ['mf_id','time_added','time_modified']])
        mf_credit_rating_df = mf_credit_rating_df.select(['mf_id','time_modified']+[col(col_name).alias('mf_credit_rating_' + col_name) for col_name in mf_credit_rating_df.columns if col_name not in ['mf_id','time_modified']])
        mf_category_data_df = mf_category_data_df.select(['time_modified']+[col(col_name).alias('mf_category_data_' + col_name) for col_name in mf_category_data_df.columns if col_name not in ['time_modified']])
        mf_category_growth_df = mf_category_growth_df.select(['mf_cat_id',col('time_added').alias('mf_category_grow_time_added'),'time_modified']+[col(col_name).alias('mf_category_growth_' + col_name) for col_name in mf_category_growth_df.columns if col_name not in ['mf_cat_id','time_added','time_modified']])
        mf_category_risk_df = mf_category_risk_df.select(['mf_cat_id','time_modified']+[col(col_name).alias('mf_category_risk_' + col_name) for col_name in mf_category_risk_df.columns if col_name not in ['mf_cat_id','time_modified']])

        mf_final_data_df = mf_data_df.join(mf_risk_df, (mf_data_df.id == mf_risk_df.mf_id) & (mf_data_df.time_added == mf_risk_df.mf_risk_time_added)) \
              .join(mf_growth_df, (mf_data_df.id == mf_growth_df.mf_id) & (mf_data_df.time_added == mf_growth_df.mf_grow_time_added)) \
              .join(mf_credit_rating_df, (mf_data_df.id == mf_credit_rating_df.mf_id) & (mf_data_df.time_added == mf_credit_rating_df.mf_credit_rating_time_added)) \
              .join(mf_category_data_df, (mf_data_df.category_id == mf_category_data_df.mf_category_data_id),  how='left') \
              .join(mf_category_growth_df, (mf_data_df.category_id == mf_category_growth_df.mf_cat_id) & (mf_data_df.time_added == mf_category_growth_df.mf_category_grow_time_added)) \
              .join(mf_category_risk_df, (mf_data_df.category_id == mf_category_risk_df.mf_cat_id) & (mf_data_df.time_added == mf_category_risk_df.mf_category_risk_time_added))       
       
        return mf_final_data_df


    def transform_data(self, df):
        """Transform original dataset.
        :param df: Input DataFrame.
        :return: Transformed DataFrame.
        """
        calculate_growth_udf = udf(lambda data: calculate_growth(data), IntegerType())
        # print(array([d_col for d_col in df.columns if d_col.find('growth')> 0]))
        df = df.withColumn("trend_growth", calculate_growth_udf(array([d_col for d_col in df.columns if d_col.find('growth')> 0])))
        df = df.withColumn("credit_rating_rank", dense_rank().over(Window.orderBy(df['mf_credit_rating_AAA'].desc())))
        df = df.withColumn("asset_value_rank", dense_rank().over(Window.orderBy(df['asset_value'].desc())))
        df = df.withColumn("expense_ratio_rank", dense_rank().over(Window.orderBy(df['expense_ratio'].asc())))
        df = df.withColumn("risk_rank", when(df.risk == 'Low', 1).when(df.risk == 'Moderately Low', 2).otherwise(3))
        # risk parameters
        df = df.withColumn("sharpe_ratio_rank", dense_rank().over(Window.orderBy(df['mf_risk_sharpe'].desc())))
        df = df.withColumn("alpha_rank", dense_rank().over(Window.orderBy(df['mf_risk_alpha'].desc())))
        df = df.withColumn("beta_rank", dense_rank().over(Window.orderBy(df['mf_risk_beta'].asc())))
        df = df.withColumn("std_dev_rank", dense_rank().over(Window.orderBy(df['mf_risk_std_dev'].asc())))
        df = df.withColumn("mean_rank", dense_rank().over(Window.orderBy(df['mf_risk_mean'].desc())))
        
        # print(df.show(1))
        df_transformed = df.withColumn('total_rank', expr('risk_rank + credit_rating_rank + asset_value_rank \
                                                        + expense_ratio_rank + sharpe_ratio_rank + alpha_rank+ \
                                                        beta_rank + std_dev_rank+ mean_rank + trend_growth'))
        return df_transformed.select('id','name', 'rating','time_added', 'total_rank').orderBy('total_rank')


    def load_data(self, df):
        """Collect data locally and write to CSV.
        :param df: DataFrame to print.
        :return: None
        """
        dateValue = str(datetime.datetime.now().strftime("%Y-%m-%d"))
        final_df = df.withColumnRenamed("id","mf_id")
        # print(final_df.show(2))
        self.save_data(final_df, 'mf_recommendation')

# entry point for PySpark ETL application
if __name__ == '__main__':
    etl = ETLJob()
    etl.main()
