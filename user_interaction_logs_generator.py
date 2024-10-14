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

# Define application pages and features
PAGES = [
    "Dashboard", "Job List", "Job Details", "System Metrics", 
    "User Management", "Settings", "Reports", "Alerts"
]

FEATURES = [
    "Start Job", "Stop Job", "View Logs", "Download Report", 
    "Create Alert", "Update Settings", "Search", "Filter Results"
]

def generate_session():
    session_id = str(uuid.uuid4())
    user_id = fake.uuid4()
    start_time = fake.date_time_this_year()
    duration = timedelta(minutes=random.randint(5, 120))
    
    interactions = []
    current_time = start_time
    current_page = random.choice(PAGES)
    
    while current_time < start_time + duration:
        time_on_page = timedelta(seconds=random.randint(10, 300))
        
        interactions.append({
            "timestamp": current_time.isoformat(),
            "action": "Page View",
            "page": current_page,
            "duration_seconds": time_on_page.seconds
        })
        
        # Simulate feature interactions on the page
        for _ in range(random.randint(0, 5)):
            feature_time = current_time + timedelta(seconds=random.randint(5, time_on_page.seconds))
            if feature_time < start_time + duration:
                interactions.append({
                    "timestamp": feature_time.isoformat(),
                    "action": "Feature Interaction",
                    "feature": random.choice(FEATURES),
                    "page": current_page
                })
        
        current_time += time_on_page
        current_page = random.choice(PAGES)
    
    return {
        "session_id": session_id,
        "user_id": user_id,
        "start_time": start_time.isoformat(),
        "duration_minutes": duration.seconds // 60,
        "interactions": interactions
    }

def generate_sessions(count=1000):
    return [generate_session() for _ in range(count)]

def index_user_interactions(sessions):
    # Create index with appropriate mappings
    index_mappings = {
        "mappings": {
            "properties": {
                "session_id": {"type": "keyword"},
                "user_id": {"type": "keyword"},
                "start_time": {"type": "date"},
                "duration_minutes": {"type": "integer"},
                "interactions": {
                    "type": "nested",
                    "properties": {
                        "timestamp": {"type": "date"},
                        "action": {"type": "keyword"},
                        "page": {"type": "keyword"},
                        "feature": {"type": "keyword"},
                        "duration_seconds": {"type": "integer"}
                    }
                }
            }
        }
    }
    
    es.indices.create(index="user_interactions", body=index_mappings, ignore=400)
    
    # Bulk index the sessions
    success, failed = bulk(es, ({"_index": "user_interactions", "_source": session} for session in sessions))
    print(f"Indexed {success} user interaction sessions. Failed: {failed}")

def generate_user_behavior_summary(sessions):
    summaries = []
    for session in sessions:
        page_views = {}
        feature_interactions = {}
        total_duration = 0
        
        for interaction in session['interactions']:
            if interaction['action'] == 'Page View':
                page_views[interaction['page']] = page_views.get(interaction['page'], 0) + 1
                total_duration += interaction['duration_seconds']
            elif interaction['action'] == 'Feature Interaction':
                feature_interactions[interaction['feature']] = feature_interactions.get(interaction['feature'], 0) + 1
        
        summaries.append({
            "user_id": session['user_id'],
            "session_id": session['session_id'],
            "start_time": session['start_time'],
            "total_duration_minutes": total_duration // 60,
            "page_view_count": sum(page_views.values()),
            "most_viewed_page": max(page_views, key=page_views.get),
            "feature_interaction_count": sum(feature_interactions.values()),
            "most_used_feature": max(feature_interactions, key=feature_interactions.get) if feature_interactions else None
        })
    
    return summaries

def index_user_behavior_summaries(summaries):
    # Create index with appropriate mappings
    index_mappings = {
        "mappings": {
            "properties": {
                "user_id": {"type": "keyword"},
                "session_id": {"type": "keyword"},
                "start_time": {"type": "date"},
                "total_duration_minutes": {"type": "integer"},
                "page_view_count": {"type": "integer"},
                "most_viewed_page": {"type": "keyword"},
                "feature_interaction_count": {"type": "integer"},
                "most_used_feature": {"type": "keyword"}
            }
        }
    }
    
    es.indices.create(index="user_behavior_summaries", body=index_mappings, ignore=400)
    
    # Bulk index the summaries
    success, failed = bulk(es, ({"_index": "user_behavior_summaries", "_source": summary} for summary in summaries))
    print(f"Indexed {success} user behavior summaries. Failed: {failed}")

if __name__ == "__main__":
    sessions = generate_sessions(1000)
    index_user_interactions(sessions)
    summaries = generate_user_behavior_summary(sessions)
    index_user_behavior_summaries(summaries)
    print("User interaction logs and behavior summaries generation and indexing complete.")