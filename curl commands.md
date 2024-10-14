$response = Invoke-RestMethod -Uri "http://localhost:9200/job_schedules/_search?pretty" -Method POST -ContentType "application/json" -Body '
{
  "query": {"match_all": {}},
  "size": 1
}
'
$response.hits.hits | ConvertTo-Json -Depth 10

# check indexes
Invoke-RestMethod -Uri "http://localhost:9200/_cat/indices?v" -Method GET

# count indexes
Invoke-RestMethod -Uri "http://localhost:9200/error_logs/_count" -Method GET

Invoke-RestMethod -Uri "http://localhost:9200/troubleshooting_guides/_count" -Method GET