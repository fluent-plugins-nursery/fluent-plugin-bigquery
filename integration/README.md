# Requirements

Set Environment Variable

- GOOGLE_APPLICATION_CREDENTIALS (json key path)
- PROJECT_NAME
- DATASET_NAME
- TABLE_NAME

# How to use

1. execute `create_table.sh`
1. `bundle exec fluentd -c fluent.conf`
1. `bundle exec dummer -c dummer_insert.rb` or `bundle exec dummer -c dummer_load.rb`
