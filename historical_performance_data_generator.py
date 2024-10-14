import random
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import uuid

# Initialize Elasticsearch client
es = Elasticsearch(["http://localhost:9200"])

# Define job types
JOB_TYPES = ["ETL", "ML Training", "Data Validation", "Report Generation", "Backup"]

# Define system configurations
SYSTEM_CONFIGS = [
    {"name": "Standard", "cpu": "4 cores", "memory": "16GB", "storage": "SSD 500GB"},
    {"name": "High CPU", "cpu": "16 cores", "memory": "32GB", "storage": "SSD 1TB"},
    {"name": "High Memory", "cpu": "8 cores", "memory": "64GB", "storage": "SSD 1TB"},
    {"name": "Storage Optimized", "cpu": "8 cores", "memory": "32GB", "storage": "SSD 2TB"},
    {"name": "All-round High Performance", "cpu": "32 cores", "memory": "128GB", "storage": "SSD 4TB"}
]

def generate_performance_data(start_date, end_date):
    current_date = start_date
    while current_date <= end_date:
        for _ in range(random.randint(50, 100)):  # Generate 50-100 jobs per day
            job_type = random.choice(JOB_TYPES)
            config = random.choice(SYSTEM_CONFIGS)
            
            # Generate performance metrics
            runtime = random.uniform(1, 120)  # Runtime in minutes
            cpu_usage = random.uniform(10, 100)
            memory_usage = random.uniform(10, 100)
            io_operations = random.randint(1000, 1000000)
            
            # Add some randomness to simulate performance variations
            time_factor = random.uniform(0.8, 1.2)
            runtime *= time_factor
            cpu_usage *= time_factor
            memory_usage *= time_factor
            
            yield {
                "job_id": str(uuid.uuid4()),
                "job_type": job_type,
                "system_config": config["name"],
                "timestamp": current_date.isoformat(),
                "runtime_minutes": round(runtime, 2),
                "cpu_usage_percent": round(cpu_usage, 2),
                "memory_usage_percent": round(memory_usage, 2),
                "io_operations": io_operations,
                "status": random.choices(["completed", "failed"], weights=[0.95, 0.05])[0]
            }
        
        current_date += timedelta(days=1)

def index_performance_data():
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
                "status": {"type": "keyword"}
            }
        }
    }
    
    es.indices.create(index="historical_performance", body=index_mappings, ignore=400)
    
    # Generate and index performance data for the last 12 months
    end_date = datetime.now()
    start_date = end_date - timedelta(days=365)
    
    # Bulk index the performance data
    success, failed = bulk(es, ({"_index": "historical_performance", "_source": data} 
                                for data in generate_performance_data(start_date, end_date)))
    print(f"Indexed {success} performance records. Failed: {failed}")

def generate_performance_summary():
    # Aggregate performance data by job type and system config
    agg_query = {
        "size": 0,
        "aggs": {
            "job_types": {
                "terms": {"field": "job_type"},
                "aggs": {
                    "configs": {
                        "terms": {"field": "system_config"},
                        "aggs": {
                            "avg_runtime": {"avg": {"field": "runtime_minutes"}},
                            "avg_cpu_usage": {"avg": {"field": "cpu_usage_percent"}},
                            "avg_memory_usage": {"avg": {"field": "memory_usage_percent"}},
                            "success_count": {"filter": {"term": {"status": "completed"}}},
                            "total_count": {"value_count": {"field": "job_id"}}
                        }
                    }
                }
            }
        }
    }
    
    results = es.search(index="historical_performance", body=agg_query)
    
    summaries = []
    for job_type in results["aggregations"]["job_types"]["buckets"]:
        for config in job_type["configs"]["buckets"]:
            success_rate = (config["success_count"]["doc_count"] / config["total_count"]["value"]) * 100 if config["total_count"]["value"] > 0 else 0
            summaries.append({
                "job_type": job_type["key"],
                "system_config": config["key"],
                "avg_runtime_minutes": round(config["avg_runtime"]["value"], 2),
                "avg_cpu_usage_percent": round(config["avg_cpu_usage"]["value"], 2),
                "avg_memory_usage_percent": round(config["avg_memory_usage"]["value"], 2),
                "success_rate_percent": round(success_rate, 2)
            })
    
    # Index performance summaries
    es.indices.create(index="performance_summaries", ignore=400)
    success, failed = bulk(es, ({"_index": "performance_summaries", "_source": summary} for summary in summaries))
    print(f"Indexed {success} performance summaries. Failed: {failed}")

if __name__ == "__main__":
    index_performance_data()
    generate_performance_summary()
    print("Historical performance data generation and indexing complete.")