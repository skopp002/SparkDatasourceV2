We have to access an enterprise API endpoint to gather Usage data available as a REST API. 

We are attempting to leverage Datasource V2 API to collect data in parallel from the endpoints. The input partitions seem to be created correctly. And the InputPartitionReader seems to have fetched the data right as well, however, it seems to keep going infinitely between the next() and get() operations of the InputPartitionReader. 
