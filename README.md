# fluent-plugin-bigquery

[Fluentd](http://fluentd.org) output plugin to load/insert data into Google BigQuery.

* insert data over streaming inserts
  * for continuous real-time insertions
  * https://developers.google.com/bigquery/streaming-data-into-bigquery#usecases
* (NOT IMPLEMENTED) load data
  * for data loading as batch jobs, for big amount of data
  * https://developers.google.com/bigquery/loading-data-into-bigquery
  
Current version of this plugin supports Google API with Service Account Authentication, but does not support
OAuth flow for installed applications.

## Configuration

### Streming inserts

Configure insert specifications with target table schema, with your credentials. This is minimum configurations:

```apache
<match dummy>
  type bigquery
  
  method insert    # default
  
  auth_method private_key   # default
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
  
  auth_method private_key   # default
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

### Authentication

There are two methods supported to fetch access token for the service account.

1. Public-Private key pair
2. Predefined access token (Compute Engine only)

The examples above use the first one.  You first need to create a service account (client ID),
download its private key and deploy the key with fluentd.

On the other hand, you don't need to explicitly create a service account for fluentd when you
run fluentd in Google Compute Engine.  In this second authentication method, you need to
add the API scope "https://www.googleapis.com/auth/bigquery" to the scope list of your
Compute Engine instance, then you can configure fluentd like this.

```apache
<match dummy>
  type bigquery
  
  auth_method compute_engine
  
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

### Table id formatting

`table` and `tables` options accept [Time#strftime](http://ruby-doc.org/core-1.9.3/Time.html#method-i-strftime)
format to construct table ids.
Table ids are formatted at runtime
using the local time of the fluentd server.

For example, with the configuration below,
data is inserted into tables `accesslog_2014_08`, `accesslog_2014_09` and so on.

```apache
<match dummy>
  type bigquery
  
  ...
  
  project yourproject_id
  dataset yourdataset_id
  table   accesslog_%Y_%m
  
  ...
</match>
```

Note that the timestamp of logs and the date in the table id do not always match,
because there is a time lag between collection and transmission of logs.

### Table schema

There are two methods to describe the schema of the target table.

1. List fields in fluent.conf
2. Load a schema file in JSON.

The examples above use the first method.  In this method,
you can also specify nested fields by prefixing their belonging record fields.

```apache
<match dummy>
  type bigquery

  ...
  
  time_format %s
  time_field  time

  field_integer time,response.status,response.bytes
  field_string  request.vhost,request.path,request.method,request.protocol,request.agent,request.referer,remote.host,remote.ip,remote.user
  field_float   request.time
  field_boolean request.bot_access,request.loginsession
</match>
```

This schema accepts structured JSON data like:

```json
{
  "request":{
    "time":1391748126.7000976,
    "vhost":"www.example.com",
    "path":"/",
    "method":"GET",
    "protocol":"HTTP/1.1",
    "agent":"HotJava",
    "bot_access":false
  },
  "remote":{ "ip": "192.0.2.1" },
  "response":{
    "status":200,
    "bytes":1024
  }
}
```

The second method is to specify a path to a BigQuery schema file instead of listing fields.  In this case, your fluent.conf looks like:

```apache
<match dummy>
  type bigquery

  ...

  time_format %s
  time_field  time

  schema_path /path/to/httpd.schema
  field_integer time
</match>
```
where /path/to/httpd.schema is a path to the JSON-encoded schema file which you used for creating the table on BigQuery.

NOTE: Since JSON does not define how to encode data of TIMESTAMP type,
you are still recommended to specify JSON types for TIMESTAMP fields as "time" field does in the example.

## TODO

* support Load API
  * with automatically configured flush/buffer options
* support optional data fields
* support NULLABLE/REQUIRED/REPEATED field options in field list style of configuration
* OAuth installed application credentials support
* Google API discovery expiration
* Error classes
* check row size limits
