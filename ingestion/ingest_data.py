import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import logging

# Add utils to path
sys.path.append('/opt/bitnami/spark/utils')
from helpers import setup_logging, create_spark_session, create_directory_structure, validate_json_files, get_file_schemas

class DataIngestion:
    def __init__(self):
        self.logger = setup_logging()
        self.spark = create_spark_session("YelpDataIngestion")
        self.schemas = get_file_schemas()
        
    def ingest_json_to_bronze(self, file_name, dataset_type):
        """Ingest JSON file to bronze layer"""
        try:
            input_path = f"/opt/bitnami/spark/data/input/{file_name}"
            output_path = f"/opt/bitnami/spark/data/bronze/{dataset_type}"
            
            self.logger.info(f"Starting ingestion of {file_name}")
            
            # Read JSON file with schema
            df = self.spark.read.option("multiline", "true").json(input_path, schema=self.schemas[dataset_type])
            
            # Add metadata columns
            df_with_metadata = df.withColumn("ingestion_timestamp", current_timestamp()) \
                                .withColumn("source_file", lit(file_name)) \
                                .withColumn("data_layer", lit("bronze"))
            
            # Write to bronze layer as parquet
            df_with_metadata.write.mode("overwrite").parquet(output_path)
            
            record_count = df_with_metadata.count()
            self.logger.info(f"Successfully ingested {record_count} records from {file_name} to bronze layer")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Error ingesting {file_name}: {str(e)}")
            return False
    
    def run_ingestion(self):
        """Run complete data ingestion process"""
        self.logger.info("Starting data ingestion process")
        
        # Create directory structure
        create_directory_structure()
        
        # Validate input files
        missing_files = validate_json_files()
        if missing_files:
            self.logger.error(f"Missing files: {missing_files}")
            return False
        
        # File mappings
        file_mappings = {
            "yelp_academic_dataset_business.json": "business",
            "yelp_academic_dataset_review.json": "review",
            "yelp_academic_dataset_user.json": "user",
            "yelp_academic_dataset_checkin.json": "checkin",
            "yelp_academic_dataset_tip.json": "tip"
        }
        
        success_count = 0
        for file_name, dataset_type in file_mappings.items():
            if self.ingest_json_to_bronze(file_name, dataset_type):
                success_count += 1
        
        self.logger.info(f"Ingestion completed. {success_count}/{len(file_mappings)} files processed successfully")
        return success_count == len(file_mappings)
    
    def stop(self):
        """Stop Spark session"""
        self.spark.stop()

if __name__ == "__main__":
    ingestion = DataIngestion()
    try:
        ingestion.run_ingestion()
    finally:
        ingestion.stop()