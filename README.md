# Yelp ETL Azure (Local Simulation)

This project simulates a robust and scalable batch-processing ETL pipeline for Yelp data using a local Docker setup that mirrors Azure services such as Data Factory, Databricks, and Synapse Analytics. The pipeline follows the bronze-silver-gold architecture to ensure clean, enriched, and aggregated data for analytics.

---

## 📦 Dataset Used

Place the following files inside the `data/input/` directory:

- yelp_academic_dataset_business.json  
- yelp_academic_dataset_checkin.json  
- yelp_academic_dataset_review.json  
- yelp_academic_dataset_tip.json  
- yelp_academic_dataset_user.json  

---

## 🚀 How to Start and Run the Project

### Step 1:
Install Docker Desktop on your local machine.

### Step 2:
Clone this repository to your local system using:  
`https://github.com/MonishaShree7391/DataEngineeringProject.git`

### Step 3:
Navigate to the project directory:  
`cd yelp-etl-azure`

### Step 4:
Place all five Yelp JSON files in the folder:  
`data/input/`

### Step 5:
Start the Docker container:  
`docker-compose up -d`

### Step 6:
List running containers to get the container ID:  
`docker ps`

### Step 7:
Access the running container shell:  
`docker exec -it <container_id> bash`

### Step 8:
Run the ingestion script to move raw data to the bronze layer:  
`python /ingestion/ingest_data.py`

### Step 9:
Run the transformation script to clean and store data into the silver layer:  
`python /transformation/preprocess.py`

### Step 10:
Run the delivery script to generate summaries into the gold layer:  
`python /delivery/load_data.py`

### Step 11:
Check the `data/bronze`, `data/silver`, and `data/gold` folders for output files.

---

## 🗂 Project Structure
yelp-etl-azure/
├── data/
│   ├── input/         # Raw Yelp JSON files
│   ├── bronze/        # Ingested raw data
│   ├── silver/        # Cleaned/processed data
│   └── gold/          # Aggregated summary outputs
├── ingestion/         # Simulates Azure Data Factory
├── transformation/    # Simulates Azure Databricks
├── delivery/          # Simulates Azure Synapse Analytics
├── utils/             # Helper functions
├── docker-compose.yml # Docker configuration
└── README.md          # Project documentation



## 🧰 Technologies Used

- Docker Desktop  
- PySpark (via Databricks runtime)  
- Pandas  
- JSON and CSV  
- Linux Terminal  
- Simulated Azure Components

---
