import os
import logging
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

def setup_logging():
    """Setup logging configuration"""
    log_dir = "/opt/bitnami/spark/logs"
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(f'{log_dir}/pipeline.log'),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

def create_spark_session(app_name="YelpETL"):
    """Create and return Spark session"""
    spark = SparkSession.builder \
        .appName(app_name) \
        .master("spark://spark-master:7077") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()
    
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
    
    spark.sparkContext.setLogLevel("WARN")
    return spark

def get_postgres_connection():
    """Get PostgreSQL connection"""
    try:
        conn = psycopg2.connect(
            host="postgres-synapse",
            port=5432,
            database="yelp_warehouse",
            user="admin",
            password="admin123"
        )
        return conn
    except Exception as e:
        print(f"Error connecting to PostgreSQL: {e}")
        return None

def get_sqlalchemy_engine():
    """Get SQLAlchemy engine for pandas operations"""
    connection_string = "postgresql://admin:admin123@postgres-synapse:5432/yelp_warehouse"
    return create_engine(connection_string)

def create_directory_structure():
    """Create necessary directory structure"""
    directories = [
        "/opt/bitnami/spark/data/input",
        "/opt/bitnami/spark/data/bronze",
        "/opt/bitnami/spark/data/silver",
        "/opt/bitnami/spark/data/gold",
        "/opt/bitnami/spark/logs"
    ]
    
    for directory in directories:
        os.makedirs(directory, exist_ok=True)

def validate_json_files():
    """Validate that all required JSON files exist"""
    required_files = [
        "yelp_academic_dataset_business.json",
        "yelp_academic_dataset_checkin.json",
        "yelp_academic_dataset_review.json",
        "yelp_academic_dataset_tip.json",
        "yelp_academic_dataset_user.json"
    ]
    
    input_path = "/opt/bitnami/spark/data/input"
    missing_files = []
    
    for file in required_files:
        if not os.path.exists(os.path.join(input_path, file)):
            missing_files.append(file)
    
    return missing_files

def get_file_schemas():
    """Define schemas for Yelp JSON files"""
    
    business_schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("postal_code", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("stars", DoubleType(), True),
        StructField("review_count", IntegerType(), True),
        StructField("is_open", IntegerType(), True),
        StructField("attributes", MapType(StringType(), StringType()), True),
        StructField("categories", StringType(), True),
        StructField("hours", MapType(StringType(), StringType()), True)
    ])
    
    review_schema = StructType([
        StructField("review_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("business_id", StringType(), True),
        StructField("stars", DoubleType(), True),
        StructField("useful", IntegerType(), True),
        StructField("funny", IntegerType(), True),
        StructField("cool", IntegerType(), True),
        StructField("text", StringType(), True),
        StructField("date", StringType(), True)
    ])
    
    user_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("review_count", IntegerType(), True),
        StructField("yelping_since", StringType(), True),
        StructField("useful", IntegerType(), True),
        StructField("funny", IntegerType(), True),
        StructField("cool", IntegerType(), True),
        StructField("elite", StringType(), True),
        StructField("friends", StringType(), True),
        StructField("fans", IntegerType(), True),
        StructField("average_stars", DoubleType(), True),
        StructField("compliment_hot", IntegerType(), True),
        StructField("compliment_more", IntegerType(), True),
        StructField("compliment_profile", IntegerType(), True),
        StructField("compliment_cute", IntegerType(), True),
        StructField("compliment_list", IntegerType(), True),
        StructField("compliment_note", IntegerType(), True),
        StructField("compliment_plain", IntegerType(), True),
        StructField("compliment_cool", IntegerType(), True),
        StructField("compliment_funny", IntegerType(), True),
        StructField("compliment_writer", IntegerType(), True),
        StructField("compliment_photos", IntegerType(), True)
    ])
    
    checkin_schema = StructType([
        StructField("business_id", StringType(), True),
        StructField("date", StringType(), True)
    ])
    
    tip_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("business_id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("date", StringType(), True),
        StructField("compliment_count", IntegerType(), True)
    ])
    
    return {
        "business": business_schema,
        "review": review_schema,
        "user": user_schema,
        "checkin": checkin_schema,
        "tip": tip_schema
    }