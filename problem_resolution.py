import random
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import faker
import uuid

# Initialize Faker for generating realistic data
fake = faker.Faker()

# Initialize Elasticsearch client
es = Elasticsearch(["http://localhost:9200"])

# Define common error types and their resolutions
ERROR_TYPES = [
    {
        "type": "DatabaseConnectionError",
        "message": "Unable to connect to the database",
        "possible_causes": [
            "Database server is down",
            "Network connectivity issues",
            "Incorrect database credentials"
        ],
        "resolution_steps": [
            "Check if the database server is running",
            "Verify network connectivity to the database server",
            "Double-check database credentials in the configuration file"
        ]
    },
    {
        "type": "OutOfMemoryError",
        "message": "Java heap space",
        "possible_causes": [
            "Insufficient memory allocated to JVM",
            "Memory leak in the application",
            "Large dataset being processed"
        ],
        "resolution_steps": [
            "Increase the heap size using -Xmx JVM argument",
            "Analyze the application for memory leaks using a profiler",
            "Implement data streaming or batching for large datasets"
        ]
    },
    # Add more error types here...
]

def generate_error_scenario():
    error = random.choice(ERROR_TYPES)
    return {
        "id": str(uuid.uuid4()),
        "error_type": error["type"],
        "error_message": error["message"],
        "timestamp": fake.date_time_this_year().isoformat(),
        "job_id": fake.uuid4(),
        "possible_causes": error["possible_causes"],
        "resolution_steps": error["resolution_steps"],
        "additional_context": fake.paragraph()
    }

def generate_troubleshooting_guide(error_type):
    guide = {
        "id": str(uuid.uuid4()),
        "error_type": error_type["type"],
        "title": f"Troubleshooting Guide for {error_type['type']}",
        "description": fake.paragraph(),
        "steps": [
            {"step": i+1, "description": step}
            for i, step in enumerate(error_type["resolution_steps"])
        ],
        "additional_resources": [
            {"title": fake.sentence(), "url": fake.url()}
            for _ in range(random.randint(1, 3))
        ]
    }
    return guide

def generate_error_logs():
    for _ in range(1000):  # Generate 1000 error logs
        yield generate_error_scenario()

def generate_troubleshooting_guides():
    for error_type in ERROR_TYPES:
        yield generate_troubleshooting_guide(error_type)

def index_data():
    # Create indices with appropriate mappings
    error_logs_mapping = {
        "mappings": {
            "properties": {
                "error_type": {"type": "keyword"},
                "error_message": {"type": "text"},
                "timestamp": {"type": "date"},
                "job_id": {"type": "keyword"},
                "possible_causes": {"type": "text"},
                "resolution_steps": {"type": "text"},
                "additional_context": {"type": "text"}
            }
        }
    }
    
    troubleshooting_guides_mapping = {
        "mappings": {
            "properties": {
                "error_type": {"type": "keyword"},
                "title": {"type": "text"},
                "description": {"type": "text"},
                "steps": {
                    "type": "nested",
                    "properties": {
                        "step": {"type": "integer"},
                        "description": {"type": "text"}
                    }
                },
                "additional_resources": {
                    "type": "nested",
                    "properties": {
                        "title": {"type": "text"},
                        "url": {"type": "keyword"}
                    }
                }
            }
        }
    }
    
    es.indices.create(index="error_logs", body=error_logs_mapping, ignore=400)
    es.indices.create(index="troubleshooting_guides", body=troubleshooting_guides_mapping, ignore=400)
    
    # Bulk index the error logs
    success, failed = bulk(es, ({"_index": "error_logs", "_source": log} for log in generate_error_logs()))
    print(f"Indexed {success} error logs. Failed: {failed}")
    
    # Bulk index the troubleshooting guides
    success, failed = bulk(es, ({"_index": "troubleshooting_guides", "_source": guide} for guide in generate_troubleshooting_guides()))
    print(f"Indexed {success} troubleshooting guides. Failed: {failed}")

if __name__ == "__main__":
    index_data()