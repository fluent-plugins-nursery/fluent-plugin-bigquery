# fluent-plugin-bigquery

Fluentd output plugin to load/insert data into Google BigQuery.

* insert data over streaming inserts
  * for continuous real-time insertions, under many limitations
  * https://developers.google.com/bigquery/streaming-data-into-bigquery#usecases
* (NOT IMPLEMENTED) load data
  * for data loading as batch jobs, for big amount of data
  * https://developers.google.com/bigquery/loading-data-into-bigquery
  
Current version of this plugin supports Google API with Service Account Authentication, and does not support OAuth.

## Configuration

### Streming inserts

For service account authentication, generate service account private key file and email key, then upload private key file onto your server.

Configure insert specifications with target table schema, with your credentials. This is minimum configurations:

```apache
<match dummy>
  type bigquery
  
  method insert    # default
  
  email xxxxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxx@developer.gserviceaccount.com
  private_key_path /home/username/.keys/00000000000000000000000000000000-privatekey.p12
  # private_key_passphrase notasecret # default
  
  project yourproject_id
  dataset yourdataset_id
  table   tablename
  
  time_format %s
  time_field  time
  
  field_integer time,status,bytes
  field_string  rhost,vhost,path,method,protocol,agent,referer
  field_float   requestime
  field_boolean bot_access,loginsession
</match>
```

For high rate inserts over streaming inserts, you should specify flush intervals and buffer chunk options:

```apache
<match dummy>
  type bigquery
  
  method insert    # default
  
  flush_interval 1  # flush as frequent as possible
  
  buffer_chunk_records_limit 300  # default rate limit for users is 100
  buffer_queue_limit 10240        # 1MB * 10240 -> 10GB!
  
  num_threads 16
  
  email xxxxxxxxxxxx-xxxxxxxxxxxxxxxxxxxxxx@developer.gserviceaccount.com
  private_key_path /home/username/.keys/00000000000000000000000000000000-privatekey.p12
  # private_key_passphrase notasecret # default
  
  project yourproject_id
  dataset yourdataset_id
  tables  accesslog1,accesslog2,accesslog3
  
  time_format %s
  time_field  time
  
  field_integer time,status,bytes
  field_string  rhost,vhost,path,method,protocol,agent,referer
  field_float   requestime
  field_boolean bot_access,loginsession
</match>
```

Important options for high rate events are:

  * `tables`
    * 2 or more tables are available with ',' separator
    * `out_bigquery` uses these tables for Table Sharding inserts
    * these must have same schema
  * `buffer_chunk_records_limit`
    * number of records over streaming inserts API call is limited as 100, per second, per table
    * default average rate limit is 100, and spike rate limit is 1000
    * `out_bigquery` flushes buffer with 100 records for 1 inserts API call
  * `buffer_queue_limit`
    * BigQuery streaming inserts needs very small buffer chunks
    * for high-rate events, `buffer_queue_limit` should be configured with big number
    * Max 1GB memory may be used under network problem in default configuration
      * `buffer_chunk_limit (default 1MB)` x `buffer_queue_limit (default 1024)`
  * `num_threads`
    * threads for insert api calls in parallel
    * specify this option for 100 or more records per seconds
    * 10 or more threads seems good for inserts over internet
    * less threads may be good for Google Compute Engine instances (with low latency for BigQuery)
  * `flush_interval`
    * `1` is lowest value, without patches on Fluentd v0.10.41 or earlier
    * see `patches` below

### patches

This plugin depends on `fluent-plugin-buffer-lightening`, and it includes monkey patch module for BufferedOutput plugin, to realize high rate and low latency flushing. With this patch, sub 1 second flushing available.

To use this feature, execute fluentd with `-r fluent/plugin/output_try_flush_interval_patch` option.
And configure `flush_interval` and `try_flush_interval` with floating point value.

```apache
<match dummy>
  type bigquery
  
  method insert    # default
  
  flush_interval     0.2
  try_flush_interval 0.05
  
  buffer_chunk_records_limit 300  # default rate limit for users is 100
  buffer_queue_limit 10240        # 1MB * 10240 -> 10GB!
  
  num_threads 16
  
  # credentials, project/dataset/table and schema specs.
</match>
```

With this configuration, flushing will be done in 0.25 seconds after record inputs in the worst case.

## TODO

* support Load API
  * with automatically configured flush/buffer options
* support RECORD field
  * and support optional data fields
* support NULLABLE/REQUIRED/REPEATED field options
* OAuth installed application credentials support
* Google API discovery expiration
* Error classes
* check row size limits
