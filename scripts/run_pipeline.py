#!/usr/bin/env python3
"""
Yelp ETL Pipeline Orchestrator
Main script to run the complete ETL pipeline
"""
import os
import sys
import time
import logging
from datetime import datetime
import subprocess

# Add project modules to path
sys.path.append('/opt/bitnami/spark')
sys.path.append('/opt/bitnami/spark/utils')
sys.path.append('/opt/bitnami/spark/ingestion')
sys.path.append('/opt/bitnami/spark/transformation')
sys.path.append('/opt/bitnami/spark/delivery')

from helpers import setup_logging, create_directory_structure, validate_json_files
from ingestion.ingest_data import DataIngestion
from transformation.preprocess import DataTransformation
from delivery.load_data import DataDelivery

class PipelineOrchestrator:
    def __init__(self):
        self.logger = setup_logging()
        self.pipeline_start_time = time.time()
        self.pipeline_id = f"yelp_etl_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        self.stages_completed = []
        self.stages_failed = []
        
    def wait_for_services(self, max_wait_time=300):
        """Wait for required services to be ready"""
        self.logger.info("Waiting for services to be ready...")
        
        services_to_check = [
            ("Spark Master", "spark-master", 8080),
            ("PostgreSQL", "postgres-synapse", 5432)
        ]
        
        start_time = time.time()
        
        for service_name, host, port in services_to_check:
            service_ready = False
            while not service_ready and (time.time() - start_time) < max_wait_time:
                try:
                    import socket
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(5)
                    result = sock.connect_ex((host, port))
                    sock.close()
                    
                    if result == 0:
                        self.logger.info(f"{service_name} is ready")
                        service_ready = True
                    else:
                        self.logger.info(f"Waiting for {service_name}...")
                        time.sleep(10)
                        
                except Exception as e:
                    self.logger.warning(f"Error checking {service_name}: {e}")
                    time.sleep(10)
            
            if not service_ready:
                self.logger.error(f"{service_name} is not ready after {max_wait_time} seconds")
                return False
        
        self.logger.info("All services are ready!")
        return True
    
    def pre_flight_checks(self):
        """Perform pre-flight checks before running pipeline"""
        self.logger.info("Performing pre-flight checks...")
        
        # Check if required directories exist
        create_directory_structure()
        
        # Validate input files
        missing_files = validate_json_files()
        if missing_files:
            self.logger.error(f"Missing input files: {missing_files}")
            self.logger.error("Please place the required Yelp JSON files in the data/input directory")
            return False
        
        # Check available disk space
        try:
            import shutil
            total, used, free = shutil.disk_usage("/opt/bitnami/spark/data")
            free_gb = free // (1024**3)
            if free_gb < 5:  # Require at least 5GB free space
                self.logger.error(f"Insufficient disk space: {free_gb}GB available, minimum 5GB required")
                return False
            self.logger.info(f"Disk space check passed: {free_gb}GB available")
        except Exception as e:
            self.logger.warning(f"Could not check disk space: {e}")
        
        self.logger.info("Pre-flight checks completed successfully")
        return True
    
    def run_ingestion_stage(self):
        """Run data ingestion stage"""
        self.logger.info("=" * 50)
        self.logger.info("STARTING INGESTION STAGE")
        self.logger.info("=" * 50)
        
        stage_start_time = time.time()
        ingestion = None
        
        try:
            ingestion = DataIngestion()
            success = ingestion.run_ingestion()
            
            if success:
                self.stages_completed.append("ingestion")
                self.logger.info("Ingestion stage completed successfully")
            else:
                self.stages_failed.append("ingestion")
                self.logger.error("Ingestion stage failed")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error in ingestion stage: {str(e)}")
            self.stages_failed.append("ingestion")
            return False
        finally:
            if ingestion:
                ingestion.stop()
            stage_time = time.time() - stage_start_time
            self.logger.info(f"Ingestion stage completed in {stage_time:.2f} seconds")
    
    def run_transformation_stage(self):
        """Run data transformation stage"""
        self.logger.info("=" * 50)
        self.logger.info("STARTING TRANSFORMATION STAGE")
        self.logger.info("=" * 50)
        
        stage_start_time = time.time()
        transformation = None
        
        try:
            transformation = DataTransformation()
            success = transformation.run_transformation()
            
            if success:
                self.stages_completed.append("transformation")
                self.logger.info("Transformation stage completed successfully")
            else:
                self.stages_failed.append("transformation")
                self.logger.error("Transformation stage failed")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error in transformation stage: {str(e)}")
            self.stages_failed.append("transformation")
            return False
        finally:
            if transformation:
                transformation.stop()
            stage_time = time.time() - stage_start_time
            self.logger.info(f"Transformation stage completed in {stage_time:.2f} seconds")
    
    def run_delivery_stage(self):
        """Run data delivery stage"""
        self.logger.info("=" * 50)
        self.logger.info("STARTING DELIVERY STAGE")
        self.logger.info("=" * 50)
        
        stage_start_time = time.time()
        delivery = None
        
        try:
            delivery = DataDelivery()
            success = delivery.run_delivery()
            
            if success:
                self.stages_completed.append("delivery")
                self.logger.info("Delivery stage completed successfully")
            else:
                self.stages_failed.append("delivery")
                self.logger.error("Delivery stage failed")
            
            return success
            
        except Exception as e:
            self.logger.error(f"Error in delivery stage: {str(e)}")
            self.stages_failed.append("delivery")
            return False
        finally:
            if delivery:
                delivery.stop()
            stage_time = time.time() - stage_start_time
            self.logger.info(f"Delivery stage completed in {stage_time:.2f} seconds")
    
    def generate_pipeline_report(self):
        """Generate pipeline execution report"""
        total_time = time.time() - self.pipeline_start_time
        
        self.logger.info("=" * 60)
        self.logger.info("PIPELINE EXECUTION REPORT")
        self.logger.info("=" * 60)
        self.logger.info(f"Pipeline ID: {self.pipeline_id}")
        self.logger.info(f"Total Execution Time: {total_time:.2f} seconds")
        self.logger.info(f"Stages Completed: {len(self.stages_completed)}")
        self.logger.info(f"Stages Failed: {len(self.stages_failed)}")
        
        if self.stages_completed:
            self.logger.info("✅ Completed Stages:")
            for stage in self.stages_completed:
                self.logger.info(f"   - {stage}")
        
        if self.stages_failed:
            self.logger.info("❌ Failed Stages:")
            for stage in self.stages_failed:
                self.logger.info(f"   - {stage}")
        
        success_rate = (len(self.stages_completed) / (len(self.stages_completed) + len(self.stages_failed))) * 100
        self.logger.info(f"Success Rate: {success_rate:.1f}%")
        
        if len(self.stages_failed) == 0:
            self.logger.info("🎉 PIPELINE COMPLETED SUCCESSFULLY!")
        else:
            self.logger.info("⚠️  PIPELINE COMPLETED WITH ERRORS")
        
        self.logger.info("=" * 60)
    
    def run_data_quality_checks(self):
        """Run data quality checks after pipeline completion"""
        self.logger.info("Running data quality checks...")
        
        try:
            from helpers import get_postgres_connection
            conn = get_postgres_connection()
            if not conn:
                self.logger.error("Could not connect to database for quality checks")
                return False
            
            cursor = conn.cursor()
            
            quality_checks = [
                {
                    "name": "Business records count",
                    "query": "SELECT COUNT(*) FROM business_analytics",
                    "min_expected": 1000
                },
                {
                    "name": "User records count", 
                    "query": "SELECT COUNT(*) FROM user_analytics",
                    "min_expected": 1000
                },
                {
                    "name": "City records count",
                    "query": "SELECT COUNT(*) FROM city_summary", 
                    "min_expected": 10
                },
                {
                    "name": "Reviews sample count",
                    "query": "SELECT COUNT(*) FROM reviews",
                    "min_expected": 1000
                },
                {
                    "name": "Business null check",
                    "query": "SELECT COUNT(*) FROM business_analytics WHERE business_id IS NULL",
                    "max_expected": 0
                },
                {
                    "name": "Rating range check",
                    "query": "SELECT COUNT(*) FROM business_analytics WHERE avg_stars < 1 OR avg_stars > 5",
                    "max_expected": 0
                }
            ]
            
            passed_checks = 0
            total_checks = len(quality_checks)
            
            for check in quality_checks:
                try:
                    cursor.execute(check["query"])
                    result = cursor.fetchone()[0]
                    
                    check_passed = False
                    if "min_expected" in check:
                        check_passed = result >= check["min_expected"]
                        self.logger.info(f"{check['name']}: {result} (min: {check['min_expected']}) - {'PASS' if check_passed else 'FAIL'}")
                    elif "max_expected" in check:
                        check_passed = result <= check["max_expected"]
                        self.logger.info(f"{check['name']}: {result} (max: {check['max_expected']}) - {'PASS' if check_passed else 'FAIL'}")
                    
                    if check_passed:
                        passed_checks += 1
                        
                except Exception as e:
                    self.logger.error(f"Error running check '{check['name']}': {e}")
            
            conn.close()
            
            quality_score = (passed_checks / total_checks) * 100
            self.logger.info(f"Data Quality Score: {quality_score:.1f}% ({passed_checks}/{total_checks} checks passed)")
            
            return quality_score >= 80  # Require 80% of checks to pass
            
        except Exception as e:
            self.logger.error(f"Error running data quality checks: {e}")
            return False
    
    def cleanup_temp_files(self):
        """Clean up temporary files after pipeline execution"""
        try:
            self.logger.info("Cleaning up temporary files...")
            
            # Clean up Spark temporary files
            temp_dirs = [
                "/tmp/spark-*",
                "/opt/bitnami/spark/work/*"
            ]
            
            for temp_pattern in temp_dirs:
                try:
                    import glob
                    for temp_file in glob.glob(temp_pattern):
                        if os.path.isdir(temp_file):
                            import shutil
                            shutil.rmtree(temp_file)
                        elif os.path.isfile(temp_file):
                            os.remove(temp_file)
                except Exception as e:
                    self.logger.warning(f"Could not clean {temp_pattern}: {e}")
            
            self.logger.info("Cleanup completed")
            
        except Exception as e:
            self.logger.warning(f"Error during cleanup: {e}")
    
    def run_pipeline(self, skip_stages=None):
        """Run the complete ETL pipeline"""
        if skip_stages is None:
            skip_stages = []
            
        self.logger.info("🚀 Starting Yelp ETL Pipeline")
        self.logger.info(f"Pipeline ID: {self.pipeline_id}")
        
        # Wait for services
        if not self.wait_for_services():
            self.logger.error("Services are not ready. Aborting pipeline.")
            return False
        
        # Pre-flight checks
        if not self.pre_flight_checks():
            self.logger.error("Pre-flight checks failed. Aborting pipeline.")
            return False
        
        pipeline_success = True
        
        # Run stages
        stages = [
            ("ingestion", self.run_ingestion_stage),
            ("transformation", self.run_transformation_stage), 
            ("delivery", self.run_delivery_stage)
        ]
        
        for stage_name, stage_func in stages:
            if stage_name in skip_stages:
                self.logger.info(f"Skipping {stage_name} stage")
                continue
                
            stage_success = stage_func()
            if not stage_success:
                pipeline_success = False
                
                # Ask if user wants to continue with remaining stages
                if stage_name != stages[-1][0]:  # Not the last stage
                    continue_pipeline = input(f"\n{stage_name} stage failed. Continue with remaining stages? (y/n): ").lower().strip()
                    if continue_pipeline != 'y':
                        self.logger.info("Pipeline aborted by user")
                        break
        
        # Run data quality checks if pipeline completed
        if pipeline_success:
            quality_passed = self.run_data_quality_checks()
            if not quality_passed:
                self.logger.warning("Data quality checks failed")
                pipeline_success = False
        
        # Generate report
        self.generate_pipeline_report()
        
        # Cleanup
        self.cleanup_temp_files()
        
        return pipeline_success

def main():
    """Main function to run the pipeline"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Yelp ETL Pipeline")
    parser.add_argument("--skip-stages", nargs="*", choices=["ingestion", "transformation", "delivery"],
                       help="Stages to skip during execution")
    parser.add_argument("--wait-for-services", type=int, default=300,
                       help="Maximum time to wait for services (seconds)")
    parser.add_argument("--dry-run", action="store_true",
                       help="Perform pre-flight checks only")
    
    args = parser.parse_args()
    
    orchestrator = PipelineOrchestrator()
    
    if args.dry_run:
        print("🔍 Running pre-flight checks only...")
        if orchestrator.pre_flight_checks():
            print("✅ Pre-flight checks passed!")
            return 0
        else:
            print("❌ Pre-flight checks failed!")
            return 1
    
    try:
        success = orchestrator.run_pipeline(skip_stages=args.skip_stages or [])
        return 0 if success else 1
        
    except KeyboardInterrupt:
        orchestrator.logger.info("Pipeline interrupted by user")
        return 130
    except Exception as e:
        orchestrator.logger.error(f"Unexpected error: {e}")
        return 1

if __name__ == "__main__":
    exit(main())