import random
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import numpy as np

# Initialize Elasticsearch client
es = Elasticsearch(["http://localhost:9200"])

# Define the start and end dates for our data
START_DATE = datetime(2023, 1, 1)
END_DATE = datetime(2023, 6, 30)

# Define the metrics we want to track
METRICS = [
    {"name": "cpu_usage", "unit": "%", "min": 0, "max": 100},
    {"name": "memory_usage", "unit": "%", "min": 0, "max": 100},
    {"name": "disk_io_read", "unit": "MB/s", "min": 0, "max": 500},
    {"name": "disk_io_write", "unit": "MB/s", "min": 0, "max": 500},
    {"name": "network_in", "unit": "Mbps", "min": 0, "max": 1000},
    {"name": "network_out", "unit": "Mbps", "min": 0, "max": 1000},
]

def generate_metric_value(metric, timestamp):
    # Generate a base value using a sine wave for some periodicity
    base_value = np.sin(timestamp.hour / 24 * 2 * np.pi) * 0.5 + 0.5
    
    # Add some random noise
    noise = np.random.normal(0, 0.1)
    
    # Scale to the metric's range and add noise
    value = base_value * (metric["max"] - metric["min"]) + metric["min"] + noise * (metric["max"] - metric["min"])
    
    # Ensure the value is within the defined range
    return max(metric["min"], min(metric["max"], value))

def generate_metrics(timestamp):
    return {
        metric["name"]: round(generate_metric_value(metric, timestamp), 2)
        for metric in METRICS
    }

def generate_system_metrics():
    current_time = START_DATE
    while current_time <= END_DATE:
        yield {
            "timestamp": current_time.isoformat(),
            "metrics": generate_metrics(current_time)
        }
        current_time += timedelta(minutes=5)  # Generate data every 5 minutes

def index_system_metrics():
    # Create index with appropriate mappings
    index_mappings = {
        "mappings": {
            "properties": {
                "timestamp": {"type": "date"},
                "metrics": {
                    "properties": {
                        metric["name"]: {"type": "float"}
                        for metric in METRICS
                    }
                }
            }
        }
    }
    
    es.indices.create(index="system_metrics", body=index_mappings, ignore=400)
    
    # Bulk index the system metrics
    def generate_actions():
        for data in generate_system_metrics():
            yield {
                "_index": "system_metrics",
                "_source": data
            }
    
    success, failed = bulk(es, generate_actions())
    print(f"Indexed {success} system metric records. Failed: {failed}")

if __name__ == "__main__":
    index_system_metrics()
    print("System metrics generation and indexing complete.")