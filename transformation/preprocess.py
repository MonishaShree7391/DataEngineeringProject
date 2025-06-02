import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

# Add utils to path
sys.path.append('/opt/bitnami/spark/utils')
from helpers import setup_logging, create_spark_session

class DataTransformation:
    def __init__(self):
        self.logger = setup_logging()
        self.spark = create_spark_session("YelpDataTransformation")
        
    def clean_business_data(self):
        """Clean and transform business data"""
        try:
            self.logger.info("Processing business data")
            
            # Read bronze data
            df = self.spark.read.parquet("/opt/bitnami/spark/data/bronze/business")
            
            # Clean and transform
            cleaned_df = df.select(
                col("business_id"),
                col("name").alias("business_name"),
                col("address"),
                col("city"),
                col("state"),
                col("postal_code"),
                col("latitude").cast("double"),
                col("longitude").cast("double"),
                col("stars").cast("double").alias("avg_stars"),
                col("review_count").cast("integer"),
                when(col("is_open") == 1, True).otherwise(False).alias("is_open"),
                col("categories"),
                col("ingestion_timestamp")
            ).filter(
                col("business_id").isNotNull() &
                col("business_name").isNotNull()
            ).withColumn("processing_timestamp", current_timestamp()) \
             .withColumn("data_layer", lit("silver"))
            
            # Write to silver layer
            cleaned_df.write.mode("overwrite").parquet("/opt/bitnami/spark/data/silver/business")
            
            count = cleaned_df.count()
            self.logger.info(f"Processed {count} business records to silver layer")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing business data: {str(e)}")
            return False
    
    def clean_review_data(self):
        """Clean and transform review data"""
        try:
            self.logger.info("Processing review data")
            
            # Read bronze data
            df = self.spark.read.parquet("/opt/bitnami/spark/data/bronze/review")
            
            # Clean and transform
            cleaned_df = df.select(
                col("review_id"),
                col("user_id"),
                col("business_id"),
                col("stars").cast("double"),
                col("useful").cast("integer"),
                col("funny").cast("integer"),
                col("cool").cast("integer"),
                col("text").alias("review_text"),
                to_date(col("date"), "yyyy-MM-dd HH:mm:ss").alias("review_date"),
                col("ingestion_timestamp")
            ).filter(
                col("review_id").isNotNull() &
                col("user_id").isNotNull() &
                col("business_id").isNotNull() &
                col("stars").isNotNull()
            ).withColumn("text_length", length(col("review_text"))) \
             .withColumn("processing_timestamp", current_timestamp()) \
             .withColumn("data_layer", lit("silver"))
            
            # Write to silver layer
            cleaned_df.write.mode("overwrite").parquet("/opt/bitnami/spark/data/silver/review")
            
            count = cleaned_df.count()
            self.logger.info(f"Processed {count} review records to silver layer")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing review data: {str(e)}")
            return False
    
    def clean_user_data(self):
        """Clean and transform user data"""
        try:
            self.logger.info("Processing user data")
            
            # Read bronze data
            df = self.spark.read.parquet("/opt/bitnami/spark/data/bronze/user")
            
            # Clean and transform
            cleaned_df = df.select(
                col("user_id"),
                col("name").alias("user_name"),
                col("review_count").cast("integer"),
                to_date(col("yelping_since"), "yyyy-MM-dd").alias("yelping_since"),
                col("useful").cast("integer"),
                col("funny").cast("integer"),
                col("cool").cast("integer"),
                col("fans").cast("integer"),
                col("average_stars").cast("double"),
                col("ingestion_timestamp")
            ).filter(
                col("user_id").isNotNull()
            ).withColumn("processing_timestamp", current_timestamp()) \
             .withColumn("data_layer", lit("silver"))
            
            # Write to silver layer
            cleaned_df.write.mode("overwrite").parquet("/opt/bitnami/spark/data/silver/user")
            
            count = cleaned_df.count()
            self.logger.info(f"Processed {count} user records to silver layer")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing user data: {str(e)}")
            return False
    
    def clean_checkin_data(self):
        """Clean and transform checkin data"""
        try:
            self.logger.info("Processing checkin data")
            
            # Read bronze data
            df = self.spark.read.parquet("/opt/bitnami/spark/data/bronze/checkin")
            
            # Split the comma-separated dates and create individual records
            cleaned_df = df.select(
                col("business_id"),
                explode(split(col("date"), ", ")).alias("checkin_date"),
                col("ingestion_timestamp")
            ).withColumn("checkin_datetime", to_timestamp(col("checkin_date"), "yyyy-MM-dd HH:mm:ss")) \
             .filter(col("business_id").isNotNull()) \
             .withColumn("processing_timestamp", current_timestamp()) \
             .withColumn("data_layer", lit("silver"))
            
            # Write to silver layer
            cleaned_df.write.mode("overwrite").parquet("/opt/bitnami/spark/data/silver/checkin")
            
            count = cleaned_df.count()
            self.logger.info(f"Processed {count} checkin records to silver layer")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing checkin data: {str(e)}")
            return False
    
    def clean_tip_data(self):
        """Clean and transform tip data"""
        try:
            self.logger.info("Processing tip data")
            
            # Read bronze data
            df = self.spark.read.parquet("/opt/bitnami/spark/data/bronze/tip")
            
            # Clean and transform
            cleaned_df = df.select(
                col("user_id"),
                col("business_id"),
                col("text").alias("tip_text"),
                to_date(col("date"), "yyyy-MM-dd HH:mm:ss").alias("tip_date"),
                col("compliment_count").cast("integer"),
                col("ingestion_timestamp")
            ).filter(
                col("user_id").isNotNull() &
                col("business_id").isNotNull()
            ).withColumn("text_length", length(col("tip_text"))) \
             .withColumn("processing_timestamp", current_timestamp()) \
             .withColumn("data_layer", lit("silver"))
            
            # Write to silver layer
            cleaned_df.write.mode("overwrite").parquet("/opt/bitnami/spark/data/silver/tip")
            
            count = cleaned_df.count()
            self.logger.info(f"Processed {count} tip records to silver layer")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing tip data: {str(e)}")
            return False
    
    def create_gold_aggregations(self):
        """Create aggregated data for gold layer"""
        try:
            self.logger.info("Creating gold layer aggregations")
            
            # Read silver data
            business_df = self.spark.read.parquet("/opt/bitnami/spark/data/silver/business")
            review_df = self.spark.read.parquet("/opt/bitnami/spark/data/silver/review")
            user_df = self.spark.read.parquet("/opt/bitnami/spark/data/silver/user")
            
            # Business Analytics
            business_analytics = business_df.join(
                review_df.groupBy("business_id").agg(
                    count("review_id").alias("actual_review_count"),
                    avg("stars").alias("avg_review_stars"),
                    sum("useful").alias("total_useful_votes"),
                    sum("funny").alias("total_funny_votes"),
                    sum("cool").alias("total_cool_votes")
                ), "business_id", "left"
            ).select(
                col("business_id"),
                col("business_name"),
                col("city"),
                col("state"),
                col("avg_stars"),
                col("review_count"),
                col("actual_review_count"),
                col("avg_review_stars"),
                col("total_useful_votes"),
                col("total_funny_votes"),
                col("total_cool_votes"),
                col("is_open"),
                col("categories")
            ).withColumn("processing_timestamp", current_timestamp()) \
             .withColumn("data_layer", lit("gold"))
            
            business_analytics.write.mode("overwrite").parquet("/opt/bitnami/spark/data/gold/business_analytics")
            
            # User Analytics
            user_analytics = user_df.join(
                review_df.groupBy("user_id").agg(
                    count("review_id").alias("actual_review_count"),
                    avg("stars").alias("avg_given_stars"),
                    sum("useful").alias("useful_votes_given"),
                    sum("funny").alias("funny_votes_given"),
                    sum("cool").alias("cool_votes_given"),
                    avg("text_length").alias("avg_review_length")
                ), "user_id", "left"
            ).select(
                col("user_id"),
                col("user_name"),
                col("review_count"),
                col("actual_review_count"),
                col("yelping_since"),
                col("average_stars"),
                col("avg_given_stars"),
                col("fans"),
                col("useful"),
                col("funny"),
                col("cool"),
                col("useful_votes_given"),
                col("funny_votes_given"),
                col("cool_votes_given"),
                col("avg_review_length")
            ).withColumn("processing_timestamp", current_timestamp()) \
             .withColumn("data_layer", lit("gold"))
            
            user_analytics.write.mode("overwrite").parquet("/opt/bitnami/spark/data/gold/user_analytics")
            
            # City-wise Business Summary
            city_summary = business_analytics.groupBy("city", "state").agg(
                count("business_id").alias("total_businesses"),
                countDistinct("business_id").alias("unique_businesses"),
                avg("avg_stars").alias("city_avg_stars"),
                sum("actual_review_count").alias("total_reviews"),
                count(when(col("is_open") == True, 1)).alias("open_businesses"),
                count(when(col("is_open") == False, 1)).alias("closed_businesses")
            ).withColumn("processing_timestamp", current_timestamp()) \
             .withColumn("data_layer", lit("gold"))
            
            city_summary.write.mode("overwrite").parquet("/opt/bitnami/spark/data/gold/city_summary")
            
            self.logger.info("Gold layer aggregations created successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error creating gold aggregations: {str(e)}")
            return False
    
    def run_transformation(self):
        """Run complete data transformation process"""
        self.logger.info("Starting data transformation process")
        
        transformations = [
            self.clean_business_data,
            self.clean_review_data,
            self.clean_user_data,
            self.clean_checkin_data,
            self.clean_tip_data,
            self.create_gold_aggregations
        ]
        
        success_count = 0
        for transformation in transformations:
            if transformation():
                success_count += 1
        
        self.logger.info(f"Transformation completed. {success_count}/{len(transformations)} processes completed successfully")
        return success_count == len(transformations)
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

if __name__ == "__main__":
    transformation = DataTransformation()
    try:
        transformation.run_transformation()
    finally:
        transformation.stop()