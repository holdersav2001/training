import random
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import faker
import uuid
import networkx as nx

# Initialize Faker for generating realistic data
fake = faker.Faker()

# Initialize Elasticsearch client
es = Elasticsearch(["http://localhost:9200"])

# Job types and their average durations (in minutes)
JOB_TYPES = {
    "ETL": 120,
    "ML Training": 240,
    "Data Validation": 60,
    "Report Generation": 30,
    "Backup": 180,
    "API Sync": 45,
    "Log Analysis": 90,
    "Database Maintenance": 150
}

def generate_job(job_id, start_time):
    job_type = random.choice(list(JOB_TYPES.keys()))
    duration = int(random.gauss(JOB_TYPES[job_type], JOB_TYPES[job_type] / 4))
    duration = max(1, duration)  # Ensure duration is at least 1 minute
    end_time = start_time + timedelta(minutes=duration)
    
    return {
        "job_id": job_id,
        "job_type": job_type,
        "planned_start_time": start_time.isoformat(),
        "planned_end_time": end_time.isoformat(),
        "duration_minutes": duration,
        "status": "scheduled"
    }

def generate_schedule():
    schedule_id = str(uuid.uuid4())
    num_jobs = random.randint(10, 50)
    
    # Create a directed acyclic graph for job dependencies
    G = nx.DiGraph()
    for i in range(num_jobs):
        G.add_node(i)
    
    # Add random edges (dependencies) ensuring no cycles
    for i in range(1, num_jobs):
        possible_deps = list(range(i))
        num_deps = random.randint(0, min(3, len(possible_deps)))
        deps = random.sample(possible_deps, num_deps)
        for dep in deps:
            G.add_edge(dep, i)
    
    # Topologically sort the graph to get a valid execution order
    execution_order = list(nx.topological_sort(G))
    
    # Generate jobs based on the dependency graph
    start_date = fake.date_time_between(start_date="-1y", end_date="+1y")
    jobs = []
    job_start_times = {}
    
    for node in execution_order:
        job_id = f"{schedule_id}-{node}"
        if node == 0:
            start_time = start_date
        else:
            # Start time is the latest end time of all dependencies
            dep_end_times = [job_start_times[f"{schedule_id}-{dep}"] for dep in G.predecessors(node)]
            start_time = max(dep_end_times) if dep_end_times else start_date
        
        job = generate_job(job_id, start_time)
        jobs.append(job)
        job_start_times[job_id] = datetime.fromisoformat(job['planned_end_time'])
    
    return {
        "schedule_id": schedule_id,
        "name": fake.bs(),
        "description": fake.paragraph(),
        "created_at": fake.date_time_this_year().isoformat(),
        "jobs": jobs,
        "dependencies": [{"source": f"{schedule_id}-{source}", "target": f"{schedule_id}-{target}"} 
                         for source, target in G.edges()]
    }

def generate_schedules(count=100):
    for _ in range(count):
        yield generate_schedule()

def index_schedules():
    # Create index with appropriate mappings
    index_mappings = {
        "mappings": {
            "properties": {
                "schedule_id": {"type": "keyword"},
                "name": {"type": "text"},
                "description": {"type": "text"},
                "created_at": {"type": "date"},
                "jobs": {
                    "type": "nested",
                    "properties": {
                        "job_id": {"type": "keyword"},
                        "job_type": {"type": "keyword"},
                        "planned_start_time": {"type": "date"},
                        "planned_end_time": {"type": "date"},
                        "duration_minutes": {"type": "integer"},
                        "status": {"type": "keyword"}
                    }
                },
                "dependencies": {
                    "type": "nested",
                    "properties": {
                        "source": {"type": "keyword"},
                        "target": {"type": "keyword"}
                    }
                }
            }
        }
    }
    
    es.indices.create(index="job_schedules", body=index_mappings, ignore=400)
    
    # Bulk index the schedules
    success, failed = bulk(es, ({"_index": "job_schedules", "_source": schedule} for schedule in generate_schedules(100)))
    print(f"Indexed {success} job schedules. Failed: {failed}")

if __name__ == "__main__":
    index_schedules()