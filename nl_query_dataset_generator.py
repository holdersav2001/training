import random
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import faker
import json

# Initialize Faker for generating realistic data
fake = faker.Faker()

# Initialize Elasticsearch client
es = Elasticsearch(["http://localhost:9200"])

# Define query templates and their corresponding answer templates
QUERY_TEMPLATES = [
    {
        "category": "job_status",
        "templates": [
            "What is the status of job {job_id}?",
            "Has job {job_id} completed?",
            "Is job {job_id} still running?",
            "When did job {job_id} start?",
            "How long has job {job_id} been running?",
        ],
        "answer_template": "Job {job_id} is currently {status}. It started at {start_time} and has been running for {duration}."
    },
    {
        "category": "performance",
        "templates": [
            "What is the average runtime of {job_type} jobs?",
            "Which {job_type} job had the longest runtime yesterday?",
            "How does the performance of job {job_id} compare to similar jobs?",
            "Are there any {job_type} jobs that are performing slower than usual?",
            "What is the success rate of {job_type} jobs in the last week?",
        ],
        "answer_template": "The average runtime of {job_type} jobs is {avg_runtime}. Job {job_id} had the longest runtime of {max_runtime}. Compared to similar jobs, job {job_id} is performing {performance_comparison}. The success rate of {job_type} jobs in the last week is {success_rate}%."
    },
    {
        "category": "error_troubleshooting",
        "templates": [
            "Why did job {job_id} fail?",
            "What are the common errors for {job_type} jobs?",
            "How can I resolve the error in job {job_id}?",
            "What steps should I take to troubleshoot the failure of job {job_id}?",
            "Are there any known issues affecting {job_type} jobs currently?",
        ],
        "answer_template": "Job {job_id} failed due to {error_reason}. Common errors for {job_type} jobs include {common_errors}. To resolve the error, try the following steps: {resolution_steps}. Currently, there are {known_issues} known issues affecting {job_type} jobs."
    },
]

def generate_query_answer_pair():
    category = random.choice(QUERY_TEMPLATES)
    query_template = random.choice(category["templates"])
    
    # Generate placeholder data
    job_id = fake.uuid4()
    job_type = random.choice(["ETL", "ML Training", "Data Validation", "Report Generation"])
    status = random.choice(["running", "completed", "failed"])
    start_time = fake.iso8601()
    duration = f"{random.randint(1, 120)} minutes"
    avg_runtime = f"{random.randint(10, 100)} minutes"
    max_runtime = f"{random.randint(30, 180)} minutes"
    performance_comparison = random.choice(["better", "worse", "average"])
    success_rate = round(random.uniform(70, 100), 2)
    error_reason = fake.sentence()
    common_errors = ", ".join(fake.words(3))
    resolution_steps = "; ".join(fake.sentences(3))
    known_issues = random.randint(0, 3)
    
    # Fill in the query and answer templates
    query = query_template.format(job_id=job_id, job_type=job_type)
    answer = category["answer_template"].format(
        job_id=job_id, job_type=job_type, status=status, start_time=start_time,
        duration=duration, avg_runtime=avg_runtime, max_runtime=max_runtime,
        performance_comparison=performance_comparison, success_rate=success_rate,
        error_reason=error_reason, common_errors=common_errors,
        resolution_steps=resolution_steps, known_issues=known_issues
    )
    
    return {
        "query": query,
        "answer": answer,
        "category": category["category"],
        "job_id": job_id,
        "job_type": job_type
    }

def generate_dataset(size=500):
    return [generate_query_answer_pair() for _ in range(size)]

def index_dataset(dataset):
    # Create index with appropriate mappings
    index_mappings = {
        "mappings": {
            "properties": {
                "query": {"type": "text"},
                "answer": {"type": "text"},
                "category": {"type": "keyword"},
                "job_id": {"type": "keyword"},
                "job_type": {"type": "keyword"}
            }
        }
    }
    
    es.indices.create(index="nl_queries", body=index_mappings, ignore=400)
    
    # Bulk index the dataset
    success, failed = bulk(es, ({"_index": "nl_queries", "_source": item} for item in dataset))
    print(f"Indexed {success} query-answer pairs. Failed: {failed}")

if __name__ == "__main__":
    dataset = generate_dataset(500)
    index_dataset(dataset)
    
    # Save a sample of the dataset to a JSON file for reference
    with open("nl_queries_sample.json", "w") as f:
        json.dump(random.sample(dataset, 10), f, indent=2)

    print("Generated 500 query-answer pairs and saved a sample to nl_queries_sample.json")