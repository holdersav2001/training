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

# Define compliance policies
COMPLIANCE_POLICIES = [
    {
        "id": "POL001",
        "name": "Data Encryption at Rest",
        "description": "All data must be encrypted when stored in the database.",
        "category": "Data Security",
        "check": lambda job: random.choice([True, False])  # Simplified check
    },
    {
        "id": "POL002",
        "name": "Access Control",
        "description": "Only authorized personnel should have access to sensitive data.",
        "category": "Access Management",
        "check": lambda job: random.choice([True, False])  # Simplified check
    },
    {
        "id": "POL003",
        "name": "Data Retention",
        "description": "Personal data must not be kept for longer than necessary.",
        "category": "Data Privacy",
        "check": lambda job: job["duration_minutes"] <= 60  # Example check
    },
    {
        "id": "POL004",
        "name": "Audit Logging",
        "description": "All data access and modifications must be logged.",
        "category": "Auditing",
        "check": lambda job: "audit_log" in job  # Example check
    },
    {
        "id": "POL005",
        "name": "Data Backup",
        "description": "Critical data must be backed up daily.",
        "category": "Data Protection",
        "check": lambda job: job["job_type"] == "Backup"  # Example check
    }
]

def generate_job_run():
    job_types = ["ETL", "Data Processing", "Reporting", "Backup", "Data Access"]
    return {
        "job_id": str(uuid.uuid4()),
        "job_type": random.choice(job_types),
        "start_time": fake.date_time_this_year().isoformat(),
        "duration_minutes": random.randint(5, 120),
        "user": fake.user_name(),
        "audit_log": random.choice([True, False])
    }

def check_compliance(job):
    compliance_results = []
    for policy in COMPLIANCE_POLICIES:
        is_compliant = policy["check"](job)
        compliance_results.append({
            "policy_id": policy["id"],
            "is_compliant": is_compliant,
            "details": f"{'Compliant' if is_compliant else 'Non-compliant'} with {policy['name']}"
        })
    return compliance_results

def generate_compliant_job_runs(count=5000):
    for _ in range(count):
        job = generate_job_run()
        compliance_results = check_compliance(job)
        job["compliance_results"] = compliance_results
        yield job

def index_compliance_data():
    # Index compliance policies
    for policy in COMPLIANCE_POLICIES:
        es.index(index="compliance_policies", id=policy["id"], body={
            "name": policy["name"],
            "description": policy["description"],
            "category": policy["category"]
        })
    print(f"Indexed {len(COMPLIANCE_POLICIES)} compliance policies.")

    # Create index with appropriate mappings for job runs
    job_runs_mapping = {
        "mappings": {
            "properties": {
                "job_id": {"type": "keyword"},
                "job_type": {"type": "keyword"},
                "start_time": {"type": "date"},
                "duration_minutes": {"type": "integer"},
                "user": {"type": "keyword"},
                "audit_log": {"type": "boolean"},
                "compliance_results": {
                    "type": "nested",
                    "properties": {
                        "policy_id": {"type": "keyword"},
                        "is_compliant": {"type": "boolean"},
                        "details": {"type": "text"}
                    }
                }
            }
        }
    }
    
    es.indices.create(index="compliant_job_runs", body=job_runs_mapping, ignore=400)
    
    # Bulk index the job runs
    success, failed = bulk(es, ({"_index": "compliant_job_runs", "_source": job} for job in generate_compliant_job_runs()))
    print(f"Indexed {success} compliant job runs. Failed: {failed}")

if __name__ == "__main__":
    index_compliance_data()
    print("Compliance policies and job runs generation and indexing complete.")