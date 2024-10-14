import random
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import faker
import uuid

# Initialize Faker for generating realistic data
fake = faker.Faker()

# Initialize Elasticsearch client
es = Elasticsearch(["http://localhost:9200"])

# Define job types and their corresponding code snippets
JOB_TYPES = {
    "ETL": {
        "unoptimized": """
def etl_job(data):
    result = []
    for item in data:
        transformed = transform(item)
        result.append(transformed)
    return result

def transform(item):
    # Slow, unoptimized transformation
    import time
    time.sleep(0.1)
    return item.upper()
        """,
        "optimized": """
from concurrent.futures import ThreadPoolExecutor

def etl_job(data):
    with ThreadPoolExecutor() as executor:
        return list(executor.map(transform, data))

def transform(item):
    return item.upper()
        """
    },
    "Data Validation": {
        "unoptimized": """
def validate_data(data):
    errors = []
    for item in data:
        if not is_valid(item):
            errors.append(f"Invalid item: {item}")
    return errors

def is_valid(item):
    # Slow, unoptimized validation
    import time
    time.sleep(0.05)
    return len(item) > 0
        """,
        "optimized": """
from concurrent.futures import ThreadPoolExecutor

def validate_data(data):
    with ThreadPoolExecutor() as executor:
        results = executor.map(is_valid, data)
    return [f"Invalid item: {item}" for item, valid in zip(data, results) if not valid]

def is_valid(item):
    return len(item) > 0
        """
    },
    "Report Generation": {
        "unoptimized": """
def generate_report(data):
    report = ""
    for item in data:
        report += format_item(item) + "\\n"
    return report

def format_item(item):
    # Slow, unoptimized formatting
    import time
    time.sleep(0.02)
    return f"Item: {item}"
        """,
        "optimized": """
from concurrent.futures import ThreadPoolExecutor

def generate_report(data):
    with ThreadPoolExecutor() as executor:
        formatted_items = executor.map(format_item, data)
    return "\\n".join(formatted_items)

def format_item(item):
    return f"Item: {item}"
        """
    }
}

def generate_performance_metrics():
    return {
        "runtime": round(random.uniform(0.5, 10.0), 2),
        "memory_usage": round(random.uniform(50, 500), 2),
        "cpu_usage": round(random.uniform(10, 100), 2)
    }

def generate_code_snippet():
    job_type = random.choice(list(JOB_TYPES.keys()))
    is_optimized = random.choice([True, False])
    
    snippet = {
        "id": str(uuid.uuid4()),
        "job_type": job_type,
        "is_optimized": is_optimized,
        "code": JOB_TYPES[job_type]["optimized" if is_optimized else "unoptimized"],
        "language": "python",
        "performance_metrics": generate_performance_metrics(),
        "created_at": fake.date_time_this_year().isoformat()
    }
    
    return snippet

def generate_code_snippets(count=200):
    return [generate_code_snippet() for _ in range(count)]

def index_code_snippets(snippets):
    # Create index with appropriate mappings
    index_mappings = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "job_type": {"type": "keyword"},
                "is_optimized": {"type": "boolean"},
                "code": {"type": "text"},
                "language": {"type": "keyword"},
                "performance_metrics": {
                    "properties": {
                        "runtime": {"type": "float"},
                        "memory_usage": {"type": "float"},
                        "cpu_usage": {"type": "float"}
                    }
                },
                "created_at": {"type": "date"}
            }
        }
    }
    
    es.indices.create(index="code_snippets", body=index_mappings, ignore=400)
    
    # Bulk index the code snippets
    success, failed = bulk(es, ({"_index": "code_snippets", "_source": snippet} for snippet in snippets))
    print(f"Indexed {success} code snippets. Failed: {failed}")

if __name__ == "__main__":
    snippets = generate_code_snippets(200)
    index_code_snippets(snippets)
    print("Code snippets generation and indexing complete.")