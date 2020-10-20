# twitter_to_redis
This application queries Twitter for given search terms and stores the results in Redis.
It can be run wherever, but best works as a k8s Deployment.

## Environment variables
If you wish to use this application, you need to supply credentials for Redis, a SQL DB, and a Twitter application.
This data is passed via environment variables.


## Tweet collection logic
The application collects all the tweets for all time periods.
It does this by storing information about the state of collection for each term in Redis.

The application also will not exceed Twitter's API rate limit for the free plan. 
It also does this by storing information about the number of queries in Redis.

## Logging
Logs are formatted for storage in Elasticsearch and sent to stdout.  
Since this app will run in k8s, k8s will automatically capture stdout at the node level.

## Monitoring
This application pushes custom metrics as a Prometheus exporter.
A Prometheus setup on a k8s node can scrape the metrics from container port 8000/