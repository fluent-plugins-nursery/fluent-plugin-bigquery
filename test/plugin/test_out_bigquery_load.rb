require 'helper'

class BigQueryLoadOutputTest < Test::Unit::TestCase
  def setup
    Fluent::Test.setup
  end

  SCHEMA_PATH = File.join(File.dirname(__FILE__), "testdata", "sudo.schema")
  CONFIG = %[
    table foo
    email foo@bar.example
    private_key_path /path/to/key
    project yourproject_id
    dataset yourdataset_id

    <buffer>
      @type memory
    </buffer>

    <inject>
    time_format %s
    time_key  time
    </inject>

    schema_path #{SCHEMA_PATH}
  ]

  API_SCOPE = "https://www.googleapis.com/auth/bigquery"

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::BigQueryLoadOutput).configure(conf)
  end

  def stub_writer(driver, stub_auth: true)
    stub.proxy(Fluent::BigQuery::Writer).new.with_any_args do |writer|
      stub(writer).get_auth { nil } if stub_auth
      yield writer
      writer
    end
  end
  
  def test_write
    driver = create_driver
    schema_fields = Fluent::BigQuery::Helper.deep_symbolize_keys(MultiJson.load(File.read(SCHEMA_PATH)))

    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(is_a(Fluent::Plugin::Buffer::Chunk)).yields(io)
    stub_writer(driver) do |writer|
      mock(writer).wait_load_job(is_a(String), "yourproject_id", "yourdataset_id", "dummy_job_id", "foo") { nil }
      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') { nil }

      mock(writer.client).insert_job('yourproject_id', {
        configuration: {
          load: {
            destination_table: {
              project_id: 'yourproject_id',
              dataset_id: 'yourdataset_id',
              table_id: 'foo',
            },
            schema: {
              fields: schema_fields,
            },
            write_disposition: "WRITE_APPEND",
            source_format: "NEWLINE_DELIMITED_JSON",
            ignore_unknown_values: false,
            max_bad_records: 0,
          }
        }
      }, {upload_source: io, content_type: "application/octet-stream"}) do
        s = stub!
        job_reference_stub = stub!
        s.job_reference { job_reference_stub }
        job_reference_stub.job_id { "dummy_job_id" }
        s
      end
    end

    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end
  end

  def test_write_with_prevent_duplicate_load
    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <buffer>
        @type memory
      </buffer>

      <inject>
      time_format %s
      time_key  time
      </inject>

      schema_path #{SCHEMA_PATH}
      prevent_duplicate_load true
    CONFIG
    schema_fields = Fluent::BigQuery::Helper.deep_symbolize_keys(MultiJson.load(File.read(SCHEMA_PATH)))

    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(is_a(Fluent::Plugin::Buffer::Chunk)).yields(io)
    stub_writer(driver) do |writer|
      mock(writer).wait_load_job(is_a(String), "yourproject_id", "yourdataset_id", "dummy_job_id", "foo") { nil }
      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') { nil }

      mock(writer.client).insert_job('yourproject_id', {
        configuration: {
          load: {
            destination_table: {
              project_id: 'yourproject_id',
              dataset_id: 'yourdataset_id',
              table_id: 'foo',
            },
            schema: {
              fields: schema_fields,
            },
            write_disposition: "WRITE_APPEND",
            source_format: "NEWLINE_DELIMITED_JSON",
            ignore_unknown_values: false,
            max_bad_records: 0,
          },
        },
        job_reference: {project_id: 'yourproject_id', job_id: satisfy { |x| x =~ /fluentd_job_.*/}} ,
      }, {upload_source: io, content_type: "application/octet-stream"}) do
        s = stub!
        job_reference_stub = stub!
        s.job_reference { job_reference_stub }
        job_reference_stub.job_id { "dummy_job_id" }
        s
      end
    end

    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end
  end

  def test_write_with_retryable_error
    driver = create_driver
    schema_fields = Fluent::BigQuery::Helper.deep_symbolize_keys(MultiJson.load(File.read(SCHEMA_PATH)))

    driver.instance_start
    tag, time, record = "tag", Time.now.to_i, {"a" => "b"}
    metadata = driver.instance.metadata_for_test(tag, time, record)
    chunk = driver.instance.buffer.generate_chunk(metadata).tap do |c|
      c.append([driver.instance.format(tag, time, record)])
    end

    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(chunk).yields(io)

    stub_writer(driver) do |writer|
      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') { nil }

      mock(writer.client).insert_job('yourproject_id', {
        configuration: {
          load: {
            destination_table: {
              project_id: 'yourproject_id',
              dataset_id: 'yourdataset_id',
              table_id: 'foo',
            },
            schema: {
              fields: schema_fields,
            },
            write_disposition: "WRITE_APPEND",
            source_format: "NEWLINE_DELIMITED_JSON",
            ignore_unknown_values: false,
            max_bad_records: 0,
          }
        }
      }, {upload_source: io, content_type: "application/octet-stream"}) do
        s = stub!
        job_reference_stub = stub!
        s.job_reference { job_reference_stub }
        job_reference_stub.job_id { "dummy_job_id" }
        s
      end

      mock(writer.client).get_job('yourproject_id', 'dummy_job_id') do
        s = stub!
        status_stub = stub!
        error_result = stub!

        s.status { status_stub }
        status_stub.state { "DONE" }
        status_stub.error_result { error_result }
        status_stub.errors { nil }
        error_result.message { "error" }
        error_result.reason { "backendError" }
        s
      end
    end

    assert_raise Fluent::BigQuery::RetryableError do
      driver.instance.write(chunk)
    end
    driver.instance_shutdown
  end

  def test_write_with_not_retryable_error
    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <buffer>
        @type memory
      </buffer>

      <inject>
      time_format %s
      time_key  time
      </inject>

      schema_path #{SCHEMA_PATH}
      <secondary>
        @type file
        path error
        utc
      </secondary>
    CONFIG
    schema_fields = Fluent::BigQuery::Helper.deep_symbolize_keys(MultiJson.load(File.read(SCHEMA_PATH)))

    driver.instance_start
    tag, time, record = "tag", Time.now.to_i, {"a" => "b"}
    metadata = driver.instance.metadata_for_test(tag, time, record)
    chunk = driver.instance.buffer.generate_chunk(metadata).tap do |c|
      c.append([driver.instance.format(tag, time, record)])
    end

    io = StringIO.new("hello")
    mock(driver.instance).create_upload_source(chunk).yields(io)
    stub_writer(driver) do |writer|
      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') { nil }

      mock(writer.client).insert_job('yourproject_id', {
        configuration: {
          load: {
            destination_table: {
              project_id: 'yourproject_id',
              dataset_id: 'yourdataset_id',
              table_id: 'foo',
            },
            schema: {
              fields: schema_fields,
            },
            write_disposition: "WRITE_APPEND",
            source_format: "NEWLINE_DELIMITED_JSON",
            ignore_unknown_values: false,
            max_bad_records: 0,
          }
        }
      }, {upload_source: io, content_type: "application/octet-stream"}) do
        s = stub!
        job_reference_stub = stub!
        s.job_reference { job_reference_stub }
        job_reference_stub.job_id { "dummy_job_id" }
        s
      end

      mock(writer.client).get_job('yourproject_id', 'dummy_job_id') do
        s = stub!
        status_stub = stub!
        error_result = stub!

        s.status { status_stub }
        status_stub.state { "DONE" }
        status_stub.error_result { error_result }
        status_stub.errors { nil }
        error_result.message { "error" }
        error_result.reason { "invalid" }
        s
      end
    end

    assert_raise Fluent::BigQuery::UnRetryableError do
      driver.instance.write(chunk)
    end
    assert_in_delta driver.instance.retry.secondary_transition_at , Time.now, 0.1
    driver.instance_shutdown
  end
end
