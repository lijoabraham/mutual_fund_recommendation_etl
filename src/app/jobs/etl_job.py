from pyspark.sql import Row
import os
import json
import datetime
import pandas as pd
import argparse
from pyspark.sql.functions import udf, array, dense_rank, expr, when, col
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType, DoubleType
from dependencies import spark as spk
from dependencies import db
from dependencies import udfs
from operator import add
from functools import reduce
from sqlalchemy import MetaData, Table
from sqlalchemy.dialects.mysql.dml import Insert


#  spark-submit --master spark://spark:7077 --files /usr/local/spark/app/configs/scrapper.json --py-files /usr/local/spark/app/packages.zip --jars=/usr/local/spark/app/dependencies/mysql-connector-j-8.0.31.jar --name arrow-spark --verbose --queue root.default /usr/local/spark/app/jobs/etl_job.py
class ETLJob(object):
    def __init__(self) -> None:
        self._config_data = self.get_config_data()
        self._db_session = db._connect_to_database(self._config_data)
        self.spark = None

    def main(self, cur_date=None):
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
        mf_category_data_df = self.read_data('mf_category_data')
        for cat in mf_category_data_df.collect():
            log.info(f"Processing data for category_id - {cat.id} | category - {cat.category}")
            data = None
            data = self.extract_data(cat.id, cur_date)
            if data.count() == 0:
                log.info(f"Empty data returned for category_id - {cat.id} | category - {cat.category}")
                continue
            data_transformed = self.transform_data(data)
            self.load_data(data_transformed)
            # break
            # exit()
        print("All done.")
        # log the success and terminate Spark application
        log.info('Spark job is finished')
        self.spark.stop()
        return True

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
        

    def extract_data(self, cat_id, cur_date=None):
        """Load data from Parquet file format.
        :param spark: Spark session object.
        :return: Spark DataFrame.
        """
        if not cur_date:
            cur_date = str(datetime.datetime.now().strftime("%Y-%m-%d"))
        mf_data_df = self.read_data('mf_data').filter(f"time_added == date'{cur_date}'").filter(f"category_id == '{cat_id}'")
        if mf_data_df.count() == 0:
            return mf_data_df
        mf_risk_df = self.read_data('mf_risk').filter(f"time_added == date'{cur_date}'")
        mf_growth_df = self.read_data('mf_growth').filter(f"time_added == date'{cur_date}'")
        mf_portfolio_agg_df = self.read_data('mf_portfolio_agg').filter(f"time_added == date'{cur_date}'")
        mf_credit_rating_df = self.read_data('mf_credit_rating').filter(f"time_added == date'{cur_date}'")
        mf_category_data_df = self.read_data('mf_category_data')
        mf_category_growth_df = self.read_data('mf_category_growth').filter(f"time_added == date'{cur_date}'")
        mf_category_risk_df = self.read_data('mf_category_risk').filter(f"time_added == date'{cur_date}'")
        mf_cat_portfolio_agg_df = self.read_data('mf_cat_portfolio_agg').filter(f"time_added == date'{cur_date}'")
        mf_cat_credit_rating_df = self.read_data('mf_cat_credit_rating').filter(f"time_added == date'{cur_date}'")

        mf_risk_df = mf_risk_df.select(['mf_id','time_modified']+[col(col_name).alias('mf_risk_' + col_name) for col_name in mf_risk_df.columns if col_name not in ['mf_id','time_modified']])
        mf_growth_df = mf_growth_df.select(['mf_id',col('time_added').alias('mf_grow_time_added'),'time_modified']+[col(col_name).alias('mf_growth_' + col_name) for col_name in mf_growth_df.columns if col_name not in ['mf_id','time_added','time_modified']])
        mf_credit_rating_df = mf_credit_rating_df.select(['mf_id','time_modified']+[col(col_name).alias('mf_credit_rating_' + col_name) for col_name in mf_credit_rating_df.columns if col_name not in ['mf_id','time_modified']])
        mf_portfolio_agg_df = mf_portfolio_agg_df.select(['mf_id','time_modified']+[col(col_name).alias('mf_portfolio_agg_' + col_name) for col_name in mf_portfolio_agg_df.columns if col_name not in ['mf_id','time_modified']])
        mf_category_data_df = mf_category_data_df.select(['time_modified']+[col(col_name).alias('mf_category_data_' + col_name) for col_name in mf_category_data_df.columns if col_name not in ['time_modified']])
        mf_category_growth_df = mf_category_growth_df.select(['mf_cat_id',col('time_added').alias('mf_category_grow_time_added'),'time_modified']+[col(col_name).alias('mf_category_growth_' + col_name) for col_name in mf_category_growth_df.columns if col_name not in ['mf_cat_id','time_added','time_modified']])
        mf_category_risk_df = mf_category_risk_df.select(['mf_cat_id','time_modified']+[col(col_name).alias('mf_category_risk_' + col_name) for col_name in mf_category_risk_df.columns if col_name not in ['mf_cat_id','time_modified']])
        mf_cat_portfolio_agg_df = mf_cat_portfolio_agg_df.select(['mf_cat_id','time_modified']+[col(col_name).alias('mf_cat_portfolio_agg_' + col_name) for col_name in mf_cat_portfolio_agg_df.columns if col_name not in ['mf_cat_id','time_modified']])
        mf_cat_credit_rating_df = mf_cat_credit_rating_df.select(['mf_cat_id','time_modified']+[col(col_name).alias('mf_cat_credit_rating_' + col_name) for col_name in mf_cat_credit_rating_df.columns if col_name not in ['mf_cat_id','time_modified']])


        mf_final_data_df = mf_data_df.join(mf_risk_df, (mf_data_df.id == mf_risk_df.mf_id) & (mf_data_df.time_added == mf_risk_df.mf_risk_time_added)) \
              .join(mf_growth_df, (mf_data_df.id == mf_growth_df.mf_id) & (mf_data_df.time_added == mf_growth_df.mf_grow_time_added)) \
              .join(mf_credit_rating_df, (mf_data_df.id == mf_credit_rating_df.mf_id) & (mf_data_df.time_added == mf_credit_rating_df.mf_credit_rating_time_added)) \
              .join(mf_portfolio_agg_df, (mf_data_df.id == mf_portfolio_agg_df.mf_id) & (mf_data_df.time_added == mf_portfolio_agg_df.mf_portfolio_agg_time_added)) \
              .join(mf_category_data_df, (mf_data_df.category_id == mf_category_data_df.mf_category_data_id),  how='left') \
              .join(mf_category_growth_df, (mf_data_df.category_id == mf_category_growth_df.mf_cat_id) & (mf_data_df.time_added == mf_category_growth_df.mf_category_grow_time_added)) \
              .join(mf_category_risk_df, (mf_data_df.category_id == mf_category_risk_df.mf_cat_id) & (mf_data_df.time_added == mf_category_risk_df.mf_category_risk_time_added)) \
              .join(mf_cat_portfolio_agg_df, (mf_data_df.category_id == mf_cat_portfolio_agg_df.mf_cat_id) & (mf_data_df.time_added == mf_cat_portfolio_agg_df.mf_cat_portfolio_agg_time_added)) \
              .join(mf_cat_credit_rating_df, (mf_data_df.category_id == mf_cat_credit_rating_df.mf_cat_id) & (mf_data_df.time_added == mf_cat_credit_rating_df.mf_cat_credit_rating_time_added))  

        # mf_final_data_df.show(1)
        mf_final_data_df = mf_final_data_df.replace(float("nan"),0)
        return mf_final_data_df


    def transform_data(self, df):
        """Transform original dataset.
        :param df: Input DataFrame.
        :return: Transformed DataFrame.
        """
        calculate_growth_udf = udf(lambda data: udfs.calculate_growth(data), IntegerType())
        # print(array([d_col for d_col in df.columns if d_col.find('growth')> 0]))
        df = df.withColumn("trend_growth", calculate_growth_udf(array([d_col for d_col in df.columns if d_col.find('growth')> 0])))
        # df.select('trend_growth').show(1)
        calculate_per_change_udf = udf(lambda data: udfs.calculate_per_change(data), IntegerType())
        # df.select([d_col for d_col in df.columns if d_col.find('credit_rating')> 0 and d_col.find('time_added')< 0]).show(1)
        df = df.withColumn("credit_rating_trend", calculate_per_change_udf(array([d_col for d_col in df.columns if d_col.find('credit_rating')> 0 and d_col.find('time_added')< 0])))
        # df.select('credit_rating_trend').show(1)
        df = df.withColumn("portfolio_agg_trend", calculate_per_change_udf(array([d_col for d_col in df.columns if d_col.find('portfolio_agg')> 0 and d_col.find('time_added')< 0])))
        
        df = udfs.min_max_analysis(df,["mf_credit_rating_aaa"],91,100) 
        df = udfs.min_max_analysis(df,['mf_credit_rating_cash_equivalent','mf_credit_rating_sov','mf_credit_rating_a1plus'],81,90)
        df = udfs.min_max_analysis(df,['mf_credit_rating_aa'],71,80)

        col_list = [d_col for d_col in df.columns if d_col.find('f_scaled')== 0]
        # print(col_list)
        df = df.withColumn('f_total',reduce(add, [col(x) for x in col_list]))
        
        df = df.withColumn("asset_value_rank", dense_rank().over(Window.orderBy(df['asset_value'].desc())))
        df = df.withColumn("expense_ratio_rank", dense_rank().over(Window.orderBy(df['expense_ratio'].asc())))
        df = df.withColumn("risk_rank", when(df.risk == 'Low', 1).when(df.risk == 'Moderately Low', 2).otherwise(3))
        # risk parameters
        df = df.withColumn("sharpe_ratio_rank", dense_rank().over(Window.orderBy(df['mf_risk_sharpe'].desc())))
        df = df.withColumn("alpha_rank", dense_rank().over(Window.orderBy(df['mf_risk_alpha'].desc())))
        df = df.withColumn("beta_rank", dense_rank().over(Window.orderBy(df['mf_risk_beta'].asc())))
        df = df.withColumn("std_dev_rank", dense_rank().over(Window.orderBy(df['mf_risk_std_dev'].asc())))
        df = df.withColumn("mean_rank", dense_rank().over(Window.orderBy(df['mf_risk_mean'].desc())))
        
        # df.select(['credit_rating_trend','f_total']).show(1)

        df_transformed = df.withColumn('total_rank', expr('risk_rank + asset_value_rank \
                                                        + expense_ratio_rank + sharpe_ratio_rank + alpha_rank+ \
                                                        beta_rank + std_dev_rank+ mean_rank + trend_growth + \
                                                            credit_rating_trend + portfolio_agg_trend + f_total'))
        # df_transformed.show(1)                                                   
        return df_transformed.select('id','name', 'category_id','rating','time_added', 'total_rank').orderBy('total_rank')


    def load_data(self, df):
        """Collect data locally and write to CSV.
        :param df: DataFrame to print.
        :return: None
        """
        final_df = df.withColumnRenamed("id","mf_id")
        final_df = final_df.withColumn("cat_rank", dense_rank().over(Window.orderBy(df['total_rank'].desc())))
        final_data = final_df.rdd.map(lambda row: row.asDict()).collect()
        print(f"\n************** Processed data for category id - {final_data[0]['category_id']} ********************\n")
        print(final_df.show(2))
        self.insert_or_update_mf_recommendation(final_data)
        # self.save_data(final_df, 'mf_recommendation')
    
    def insert_or_update_mf_recommendation(self,upsert_data):
        metadata = MetaData()
        mf_recommendation = Table('mf_recommendation', metadata, autoload=True, autoload_with=self._db_session.get_bind())
        insert_stmt = Insert(mf_recommendation).values(upsert_data)

        update_dict = {x.name: x for x in insert_stmt.inserted if x.name not in ['mf_id','category_id','time_added']}
        upsert_query = insert_stmt.on_duplicate_key_update(update_dict)
        self._db_session.execute(upsert_query)
        self._db_session.commit()
    
    def find_rank_within_category(self, cur_date, cat_id):
        mf_recommendation_df = self.read_data('mf_recommendation').filter(f"category_id == '{cat_id}'").filter(f"time_added == date'{cur_date}'")
        # mf_recommendation_df.show(2)
        ranked =  mf_recommendation_df.withColumn("cat_rank", dense_rank().over(Window.orderBy(mf_recommendation_df['total_rank'].desc())))
        ranked.show(20)
        return ranked

# entry point for PySpark ETL application
if __name__ == '__main__':
    
    parser = argparse.ArgumentParser()
    parser.add_argument('--cur_date', dest='cur_date', type=str, help='Which date data to be processed')
    args = parser.parse_args()
    etl = ETLJob()
    etl.main(args.cur_date)
