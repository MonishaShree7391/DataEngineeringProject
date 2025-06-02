#!/usr/bin/env python3
"""
Yelp ETL Pipeline Setup Script
Sets up the environment and validates the configuration
"""
import os
import sys
import subprocess
import time
import json
from pathlib import Path

def print_banner():
    """Print setup banner"""
    banner = """
    ╔══════════════════════════════════════════════════════════════╗
    ║                     YELP ETL PIPELINE SETUP                 ║
    ║                                                              ║
    ║  Azure-simulated ETL Pipeline for Yelp Academic Dataset     ║
    ║  Components: Spark, PostgreSQL, Jupyter, MinIO              ║
    ╚══════════════════════════════════════════════════════════════╝
    """
    print(banner)

def check_docker():
    """Check if Docker and Docker Compose are installed"""
    print("🔍 Checking Docker installation...")
    
    try:
        result = subprocess.run(["docker", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Docker found: {result.stdout.strip()}")
        else:
            print("❌ Docker not found")
            return False
    except FileNotFoundError:
        print("❌ Docker not found")
        return False
    
    try:
        result = subprocess.run(["docker-compose", "--version"], capture_output=True, text=True)
        if result.returncode == 0:
            print(f"✅ Docker Compose found: {result.stdout.strip()}")
            return True
        else:
            # Try docker compose (newer syntax)
            result = subprocess.run(["docker", "compose", "version"], capture_output=True, text=True)
            if result.returncode == 0:
                print(f"✅ Docker Compose found: {result.stdout.strip()}")
                return True
            else:
                print("❌ Docker Compose not found")
                return False
    except FileNotFoundError:
        print("❌ Docker Compose not found")
        return False

def create_directories():
    """Create required directory structure"""
    print("📁 Creating directory structure...")
    
    directories = [
        "data/input",
        "data/bronze", 
        "data/silver",
        "data/gold",
        "logs",
        "notebooks",
        "scripts",
        "sql",
        "utils",
        "ingestion",
        "transformation",
        "delivery"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"   ✅ Created: {directory}")
    
    return True

def create_init_files():
    """Create __init__.py files for Python modules"""
    print("🐍 Creating Python module files...")
    
    modules = ["utils", "ingestion", "transformation", "delivery"]
    
    for module in modules:
        init_file = Path(module) / "__init__.py"
        init_file.touch()
        print(f"   ✅ Created: {init_file}")
    
    return True

def validate_environment_file():
    """Validate .env file exists and has required variables"""
    print("⚙️  Checking environment configuration...")
    
    env_file = Path(".env")
    if not env_file.exists():
        print("❌ .env file not found")
        return False
    
    required_vars = [
        "POSTGRES_HOST",
        "POSTGRES_DB", 
        "POSTGRES_USER",
        "POSTGRES_PASSWORD",
        "SPARK_MASTER_URL"
    ]
    
    env_content = env_file.read_text()
    missing_vars = []
    
    for var in required_vars:
        if var not in env_content:
            missing_vars.append(var)
    
    if missing_vars:
        print(f"❌ Missing environment variables: {missing_vars}")
        return False
    
    print("✅ Environment file validated")
    return True

def check_yelp_dataset():
    """Check if Yelp dataset files are available"""
    print("📊 Checking Yelp dataset files...")
    
    required_files = [
        "yelp_academic_dataset_business.json",
        "yelp_academic_dataset_checkin.json", 
        "yelp_academic_dataset_review.json",
        "yelp_academic_dataset_tip.json",
        "yelp_academic_dataset_user.json"
    ]
    
    input_dir = Path("data/input")
    missing_files = []
    found_files = []
    
    for file_name in required_files:
        file_path = input_dir / file_name
        if file_path.exists():
            file_size = file_path.stat().st_size / (1024 * 1024)  # Size in MB
            found_files.append(f"{file_name} ({file_size:.1f} MB)")
            print(f"   ✅ Found: {file_name} ({file_size:.1f} MB)")
        else:
            missing_files.append(file_name)
            print(f"   ❌ Missing: {file_name}")
    
    if missing_files:
        print("\n📥 To get the Yelp dataset:")
        print("   1. Go to https://www.yelp.com/dataset")
        print("   2. Download the Yelp Academic Dataset")
        print("   3. Extract the JSON files to the data/input/ directory")
        print(f"   4. Missing files: {missing_files}")
        return False
    
    print(f"✅ All {len(found_files)} dataset files found")
    return True

def validate_docker_compose():
    """Validate docker-compose.yml file"""
    print("🐳 Validating Docker Compose configuration...")
    
    compose_file = Path("docker-compose.yml")
    if not compose_file.exists():
        print("❌ docker-compose.yml not found")
        return False
    
    try:
        result = subprocess.run(
            ["docker-compose", "config"], 
            capture_output=True, 
            text=True,
            cwd="."
        )
        if result.returncode != 0:
            # Try newer docker compose syntax
            result = subprocess.run(
                ["docker", "compose", "config"],
                capture_output=True,
                text=True, 
                cwd="."
            )
        
        if result.returncode == 0:
            print("✅ Docker Compose configuration valid")
            return True
        else:
            print(f"❌ Docker Compose validation failed: {result.stderr}")
            return False
            
    except Exception as e:
        print(f"❌ Error validating Docker Compose: {e}")
        return False

def check_system_resources():
    """Check system resources"""
    print("💻 Checking system resources...")
    
    try:
        import psutil
        
        # Check RAM
        ram = psutil.virtual_memory()
        ram_gb = ram.total / (1024**3)
        print(f"   RAM: {ram_gb:.1f} GB total, {ram.percent}% used")
        
        if ram_gb < 8:
            print("   ⚠️  Warning: Less than 8GB RAM detected. Pipeline may run slowly.")
        
        # Check disk space
        disk = psutil.disk_usage('.')
        disk_free_gb = disk.free / (1024**3)
        print(f"   Disk: {disk_free_gb:.1f} GB free")
        
        if disk_free_gb < 10:
            print("   ⚠️  Warning: Less than 10GB free disk space. May not be sufficient for large datasets.")
        
        # Check CPU
        cpu_count = psutil.cpu_count()
        print(f"   CPU: {cpu_count} cores")
        
        return True
        
    except ImportError:
        print("   ℹ️  psutil not available, skipping system resource check")
        return True
    except Exception as e:
        print(f"   ⚠️  Error checking system resources: {e}")
        return True

def create_sample_dataset():
    """Create a small sample dataset for testing"""
    print("📋 Creating sample dataset for testing...")
    
    try:
        sample_data = {
            "business": [
                {
                    "business_id": "sample_business_1",
                    "name": "Sample Restaurant",
                    "address": "123 Main St", 
                    "city": "Phoenix",
                    "state": "AZ",
                    "postal_code": "85001",
                    "latitude": 33.4484,
                    "longitude": -112.0740,
                    "stars": 4.0,
                    "review_count": 100,
                    "is_open": 1,
                    "categories": "Restaurants, Italian"
                }
            ],
            "review": [
                {
                    "review_id": "sample_review_1",
                    "user_id": "sample_user_1", 
                    "business_id": "sample_business_1",
                    "stars": 4.0,
                    "useful": 1,
                    "funny": 0,
                    "cool": 1,
                    "text": "Great food and service!",
                    "date": "2023-01-01 12:00:00"
                }
            ],
            "user": [
                {
                    "user_id": "sample_user_1",
                    "name": "John Doe",
                    "review_count": 10,
                    "yelping_since": "2020-01-01",
                    "useful": 5,
                    "funny": 2,
                    "cool": 3,
                    "fans": 1,
                    "average_stars": 4.2
                }
            ]
        }
        
        sample_dir = Path("data/sample")
        sample_dir.mkdir(exist_ok=True)
        
        for dataset_name, data in sample_data.items():
            sample_file = sample_dir / f"sample_{dataset_name}.json"
            with open(sample_file, 'w') as f:
                for record in data:
                    f.write(json.dumps(record) + '\n')
            print(f"   ✅ Created: {sample_file}")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Error creating sample dataset: {e}")
        return False

def print_next_steps():
    """Print next steps for the user"""
    print("\n" + "="*60)
    print("🎉 SETUP COMPLETED!")
    print("="*60)
    print("\n📋 Next Steps:")
    print("   1. Start the services:")
    print("      docker-compose up -d")
    print("")
    print("   2. Wait for services to be ready (2-3 minutes)")
    print("")
    print("   3. Run the ETL pipeline:")
    print("      docker exec spark-master python /opt/bitnami/spark/scripts/run_pipeline.py")
    print("")
    print("   4. Access the applications:")
    print("      • Spark UI: http://localhost:8080")
    print("      • Jupyter Lab: http://localhost:8888")
    print("      • MinIO Console: http://localhost:9001")
    print("")
    print("   5. Connect to PostgreSQL:")
    print("      Host: localhost:5432")
    print("      Database: yelp_warehouse")
    print("      User: admin")
    print("      Password: admin123")
    print("")
    print("📚 Documentation:")
    print("   • Check README.md for detailed instructions")
    print("   • View pipeline logs in the logs/ directory")
    print("   • Explore notebooks/ for analysis examples")

def main():
    """Main setup function"""
    print_banner()
    
    checks = [
        ("Docker Installation", check_docker),
        ("Directory Structure", create_directories),
        ("Python Modules", create_init_files),
        ("Environment Configuration", validate_environment_file),
        ("Docker Compose Configuration", validate_docker_compose),
        ("System Resources", check_system_resources),
        ("Sample Dataset", create_sample_dataset)
    ]
    
    passed_checks = 0
    total_checks = len(checks)
    
    for check_name, check_func in checks:
        print(f"\n{check_name}:")
        try:
            if check_func():
                passed_checks += 1
            else:
                print(f"   ❌ {check_name} failed")
        except Exception as e:
            print(f"   ❌ {check_name} failed with error: {e}")
    
    # Optional check for Yelp dataset (not required for setup)
    print(f"\nYelp Dataset (Optional):")
    dataset_available = check_yelp_dataset()
    
    print(f"\n" + "="*60)
    print(f"SETUP SUMMARY: {passed_checks}/{total_checks} checks passed")
    print("="*60)
    
    if passed_checks == total_checks:
        print("✅ Setup completed successfully!")
        if not dataset_available:
            print("ℹ️  Yelp dataset not found - you can still test with sample data")
        print_next_steps()
        return 0
    else:
        print("❌ Setup completed with errors")
        print("   Please fix the failed checks before proceeding")
        return 1

if __name__ == "__main__":
    exit(main())