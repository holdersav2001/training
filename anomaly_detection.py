from elasticsearch import Elasticsearch
import numpy as np

es = Elasticsearch(["http://localhost:9200"])

def calculate_baselines():
    query = {
        "size": 0,
        "aggs": {
            "job_types": {
                "terms": {"field": "job_type"},
                "aggs": {
                    "configs": {
                        "terms": {"field": "system_config"},
                        "aggs": {
                            "runtime_stats": {"extended_stats": {"field": "runtime_minutes"}},
                            "cpu_stats": {"extended_stats": {"field": "cpu_usage_percent"}},
                            "memory_stats": {"extended_stats": {"field": "memory_usage_percent"}}
                        }
                    }
                }
            }
        }
    }
    
    results = es.search(index="historical_performance", body=query)
    baselines = {}
    
    for job_type in results["aggregations"]["job_types"]["buckets"]:
        for config in job_type["configs"]["buckets"]:
            key = (job_type["key"], config["key"])
            baselines[key] = {
                "runtime": (config["runtime_stats"]["avg"], config["runtime_stats"]["std_deviation"]),
                "cpu_usage": (config["cpu_stats"]["avg"], config["cpu_stats"]["std_deviation"]),
                "memory_usage": (config["memory_stats"]["avg"], config["memory_stats"]["std_deviation"])
            }
    
    return baselines

def detect_anomaly(job, baselines, threshold=3):
    key = (job["job_type"], job["system_config"])
    if key not in baselines:
        return False, "No baseline for this job type and system configuration"
    
    baseline = baselines[key]
    anomalies = []
    
    for metric in ["runtime", "cpu_usage", "memory_usage"]:
        job_value = job[f"{metric}_minutes" if metric == "runtime" else f"{metric}_percent"]
        mean, std = baseline[metric]
        z_score = (job_value - mean) / std if std > 0 else 0
        
        if abs(z_score) > threshold:
            anomalies.append(f"{metric.replace('_', ' ').title()} (Z-score: {z_score:.2f})")
    
    is_anomaly = len(anomalies) > 0
    details = ", ".join(anomalies) if is_anomaly else "No anomalies detected"
    
    return is_anomaly, details

# Usage
baselines = calculate_baselines()

# Example job run
new_job = {
    "job_type": "ETL",
    "system_config": "Standard",
    "runtime_minutes": 100,
    "cpu_usage_percent": 80,
    "memory_usage_percent": 70
}

is_anomaly, details = detect_anomaly(new_job, baselines)
print(f"Anomaly detected: {is_anomaly}")
print(f"Details: {details}")