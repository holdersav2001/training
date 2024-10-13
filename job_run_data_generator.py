import random
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import faker
import uuid

# Initialize Faker for generating realistic data
fake = faker.Faker()

# Initialize Elasticsearch client
es = Elasticsearch(["http://localhost:9200"])

def generate_job_run():
    job_id = str(uuid.uuid4())
    start_time = fake.date_time_between(start_date="-6M", end_date="now")
    duration = timedelta(minutes=random.randint(1, 1440))  # 1 minute to 24 hours
    end_time = start_time + duration
    
    status = random.choices(["completed", "failed", "running"], weights=[80, 15, 5])[0]
    
    cpu_usage = random.uniform(0, 100)
    memory_usage = random.uniform(0, 32)  # GB
    disk_io = random.uniform(0, 500)  # MB/s
    
    log_verbosity = random.choice(["low", "medium", "high"])
    log_entries = generate_log_entries(start_time, end_time, log_verbosity)
    
    return {
        "job_id": job_id,
        "start_time": start_time.isoformat(),
        "end_time": end_time.isoformat(),
        "duration_minutes": duration.total_seconds() / 60,
        "status": status,
        "cpu_usage": cpu_usage,
        "memory_usage": memory_usage,
        "disk_io": disk_io,
        "log_verbosity": log_verbosity,
        "logs": log_entries
    }

def generate_log_entries(start_time, end_time, verbosity):
    log_count = {
        "low": random.randint(5, 20),
        "medium": random.randint(20, 50),
        "high": random.randint(50, 200)
    }[verbosity]
    
    log_entries = []
    for _ in range(log_count):
        timestamp = fake.date_time_between(start_date=start_time, end_date=end_time)
        level = random.choice(["INFO", "WARNING", "ERROR"])
        message = fake.sentence()
        log_entries.append(f"{timestamp.isoformat()} [{level}] {message}")
    
    return "\n".join(log_entries)

def generate_job_runs(count=10000):
    for _ in range(count):
        yield generate_job_run()

def index_job_runs():
    def generate_actions():
        for job_run in generate_job_runs():
            yield {
                "_index": "job_runs",
                "_source": job_run
            }
    
    # Create the index with appropriate mappings
    index_mappings = {
        "mappings": {
            "properties": {
                "job_id": {"type": "keyword"},
                "start_time": {"type": "date"},
                "end_time": {"type": "date"},
                "duration_minutes": {"type": "float"},
                "status": {"type": "keyword"},
                "cpu_usage": {"type": "float"},
                "memory_usage": {"type": "float"},
                "disk_io": {"type": "float"},
                "log_verbosity": {"type": "keyword"},
                "logs": {"type": "text"}
            }
        }
    }
    
    es.indices.create(index="job_runs", body=index_mappings, ignore=400)
    
    # Bulk index the job runs
    success, failed = bulk(es, generate_actions())
    print(f"Indexed {success} job runs. Failed: {failed}")

if __name__ == "__main__":
    index_job_runs()