import random
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import faker
import uuid
from datetime import datetime, timedelta

# Initialize Faker for generating realistic data
fake = faker.Faker()

# Initialize Elasticsearch client
es = Elasticsearch(["http://localhost:9200"])

# Define system components
SYSTEM_COMPONENTS = [
    {
        "name": "Job Scheduler",
        "description": "Manages and schedules all system jobs",
        "subcomponents": ["Task Queue", "Job Prioritizer", "Resource Allocator"]
    },
    {
        "name": "Data Pipeline",
        "description": "Handles data ingestion, processing, and storage",
        "subcomponents": ["Data Ingestion", "Data Transformation", "Data Storage"]
    },
    {
        "name": "Monitoring Dashboard",
        "description": "Provides real-time system and job status visualization",
        "subcomponents": ["Performance Metrics", "Job Status Tracker", "Alert System"]
    },
    {
        "name": "User Management",
        "description": "Manages user accounts, roles, and permissions",
        "subcomponents": ["Authentication", "Authorization", "User Profile Management"]
    },
    {
        "name": "Reporting Module",
        "description": "Generates various system and job reports",
        "subcomponents": ["Report Generator", "Report Scheduler", "Report Template Manager"]
    }
]

def generate_component_outline(component):
    outline = {
        "id": str(uuid.uuid4()),
        "name": component["name"],
        "description": component["description"],
        "subcomponents": component["subcomponents"],
        "technical_details": [
            f"Technical detail {i+1} for {component['name']}"
            for i in range(random.randint(3, 7))
        ],
        "api_endpoints": [
            f"/api/{component['name'].lower().replace(' ', '_')}/{fake.word()}"
            for _ in range(random.randint(2, 5))
        ],
        "related_components": random.sample([c["name"] for c in SYSTEM_COMPONENTS if c != component], k=random.randint(1, 3)),
        "last_updated": fake.date_time_this_year().isoformat()
    }
    return outline

def generate_usage_statistics(component_name):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    stats = []
    current_date = start_date
    while current_date <= end_date:
        stats.append({
            "date": current_date.isoformat(),
            "views": random.randint(10, 100),
            "unique_users": random.randint(5, 50),
            "average_time_spent": round(random.uniform(1, 10), 2)
        })
        current_date += timedelta(days=1)
    return {
        "component_name": component_name,
        "statistics": stats
    }

def generate_documentation_components():
    components = []
    usage_stats = []
    for component in SYSTEM_COMPONENTS:
        components.append(generate_component_outline(component))
        usage_stats.append(generate_usage_statistics(component["name"]))
    return components, usage_stats

def index_documentation_data(components, usage_stats):
    # Create index with appropriate mappings for components
    component_mappings = {
        "mappings": {
            "properties": {
                "id": {"type": "keyword"},
                "name": {"type": "keyword"},
                "description": {"type": "text"},
                "subcomponents": {"type": "keyword"},
                "technical_details": {"type": "text"},
                "api_endpoints": {"type": "keyword"},
                "related_components": {"type": "keyword"},
                "last_updated": {"type": "date"}
            }
        }
    }
    
    es.indices.create(index="documentation_components", body=component_mappings, ignore=400)
    
    # Bulk index the components
    success, failed = bulk(es, ({"_index": "documentation_components", "_source": component} for component in components))
    print(f"Indexed {success} documentation components. Failed: {failed}")

    # Create index with appropriate mappings for usage statistics
    usage_stats_mappings = {
        "mappings": {
            "properties": {
                "component_name": {"type": "keyword"},
                "statistics": {
                    "type": "nested",
                    "properties": {
                        "date": {"type": "date"},
                        "views": {"type": "integer"},
                        "unique_users": {"type": "integer"},
                        "average_time_spent": {"type": "float"}
                    }
                }
            }
        }
    }
    
    es.indices.create(index="documentation_usage_stats", body=usage_stats_mappings, ignore=400)
    
    # Bulk index the usage statistics
    success, failed = bulk(es, ({"_index": "documentation_usage_stats", "_source": stat} for stat in usage_stats))
    print(f"Indexed {success} documentation usage statistics. Failed: {failed}")

if __name__ == "__main__":
    components, usage_stats = generate_documentation_components()
    index_documentation_data(components, usage_stats)
    print("Documentation components and usage statistics generation and indexing complete.")