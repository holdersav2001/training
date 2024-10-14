import random
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import uuid

# Initialize Elasticsearch client
es = Elasticsearch(["http://localhost:9200"])

# Define job types and system configurations
JOB_TYPES = ["ETL", "ML Training", "Data Validation", "Report Generation", "Backup"]
SYSTEM_CONFIGS = ["Standard", "High CPU", "High Memory", "Storage Optimized", "All-round High Performance"]

# Define normal ranges for metrics
NORMAL_RANGES = {
    "runtime_minutes": (10, 120),
    "cpu_usage_percent": (10, 80),
    "memory_usage_percent": (20, 70),
    "io_operations": (1000, 100000)
}

def generate_normal_job():
    return {
        "job_id": str(uuid.uuid4()),
        "job_type": random.choice(JOB_TYPES),
        "system_config": random.choice(SYSTEM_CONFIGS),
        "timestamp": datetime.now().isoformat(),
        "runtime_minutes": random.uniform(*NORMAL_RANGES["runtime_minutes"]),
        "cpu_usage_percent": random.uniform(*NORMAL_RANGES["cpu_usage_percent"]),
        "memory_usage_percent": random.uniform(*NORMAL_RANGES["memory_usage_percent"]),
        "io_operations": random.randint(*NORMAL_RANGES["io_operations"]),
        "status": "completed"
    }

def generate_anomalous_job():
    job = generate_normal_job()
    anomaly_type = random.choice(["runtime", "cpu", "memory", "io", "failure"])
    
    if anomaly_type == "runtime":
        job["runtime_minutes"] *= random.uniform(5, 10)
    elif anomaly_type == "cpu":
        job["cpu_usage_percent"] = random.uniform(95, 100)
    elif anomaly_type == "memory":
        job["memory_usage_percent"] = random.uniform(95, 100)
    elif anomaly_type == "io":
        job["io_operations"] *= random.randint(10, 100)
    elif anomaly_type == "failure":
        job["status"] = "failed"
        job["runtime_minutes"] *= random.uniform(0.1, 0.5)
    
    job["anomaly_type"] = anomaly_type
    return job

def generate_dataset(total_jobs=1000, num_anomalies=10):
    jobs = [generate_normal_job() for _ in range(total_jobs - num_anomalies)]
    anomalies = [generate_anomalous_job() for _ in range(num_anomalies)]
    all_jobs = jobs + anomalies
    random.shuffle(all_jobs)
    return all_jobs

def index_jobs(jobs):
    # Create index with appropriate mappings
    index_mappings = {
        "mappings": {
            "properties": {
                "job_id": {"type": "keyword"},
                "job_type": {"type": "keyword"},
                "system_config": {"type": "keyword"},
                "timestamp": {"type": "date"},
                "runtime_minutes": {"type": "float"},
                "cpu_usage_percent": {"type": "float"},
                "memory_usage_percent": {"type": "float"},
                "io_operations": {"type": "long"},
                "status": {"type": "keyword"},
                "anomaly_type": {"type": "keyword"}
            }
        }
    }
    
    es.indices.create(index="job_runs_with_anomalies", body=index_mappings, ignore=400)
    
    # Bulk index the jobs
    success, failed = bulk(es, ({"_index": "job_runs_with_anomalies", "_source": job} for job in jobs))
    print(f"Indexed {success} jobs. Failed: {failed}")

def main():
    total_jobs = 1000
    num_anomalies = 15  # Generating 15 anomalies to ensure at least 10
    
    print(f"Generating dataset with {total_jobs} total jobs and {num_anomalies} anomalies...")
    jobs = generate_dataset(total_jobs, num_anomalies)
    
    print("Indexing jobs into Elasticsearch...")
    index_jobs(jobs)
    
    print("Data generation complete.")
    print("Anomalies generated:")
    for job in jobs:
        if "anomaly_type" in job:
            print(f"Job ID: {job['job_id']}, Anomaly Type: {job['anomaly_type']}")

if __name__ == "__main__":
    main()