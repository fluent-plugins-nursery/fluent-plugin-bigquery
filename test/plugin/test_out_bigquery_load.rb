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
    wait_job_interval 0.1
  ]

  API_SCOPE = "https://www.googleapis.com/auth/bigquery"

  def create_driver(conf = CONFIG)
    Fluent::Test::Driver::Output.new(Fluent::Plugin::BigQueryLoadOutput).configure(conf)
  end

  def stub_writer(stub_auth: true)
    stub.proxy(Fluent::BigQuery::Writer).new.with_any_args do |writer|
      stub(writer).get_auth { nil } if stub_auth
      yield writer
      writer
    end
  end

  def test_write
    response_stub = stub!

    driver = create_driver
    stub_writer do |writer|
      mock(writer).fetch_load_job(is_a(Fluent::BigQuery::Writer::JobReference)) { response_stub }
      mock(writer).commit_load_job(is_a(String), response_stub)

      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') { nil }

      mock(writer.client).insert_job('yourproject_id', {
        configuration: {
          load: {
            destination_table: {
              project_id: 'yourproject_id',
              dataset_id: 'yourdataset_id',
              table_id: 'foo',
            },
            write_disposition: "WRITE_APPEND",
            source_format: "NEWLINE_DELIMITED_JSON",
            ignore_unknown_values: false,
            max_bad_records: 0,
          }
        }
      }, upload_source: duck_type(:write, :sync, :rewind), content_type: "application/octet-stream") do
        stub!.job_reference.stub!.job_id { "dummy_job_id" }
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

    response_stub = stub!
    stub_writer do |writer|
      mock(writer).fetch_load_job(is_a(Fluent::BigQuery::Writer::JobReference)) { response_stub }
      mock(writer).commit_load_job(is_a(String), response_stub)

      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') { nil }

      mock(writer.client).insert_job('yourproject_id', {
        configuration: {
          load: {
            destination_table: {
              project_id: 'yourproject_id',
              dataset_id: 'yourdataset_id',
              table_id: 'foo',
            },
            write_disposition: "WRITE_APPEND",
            source_format: "NEWLINE_DELIMITED_JSON",
            ignore_unknown_values: false,
            max_bad_records: 0,
          },
        },
        job_reference: {project_id: 'yourproject_id', job_id: satisfy { |x| x =~ /fluentd_job_.*/}} ,
      }, upload_source: duck_type(:write, :sync, :rewind), content_type: "application/octet-stream") do
        stub!.job_reference.stub!.job_id { "dummy_job_id" }
      end
    end

    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end
  end

  def test_write_with_retryable_error
    driver = create_driver

    driver.instance_start
    tag, time, record = "tag", Time.now.to_i, {"a" => "b"}
    metadata = Fluent::Plugin::Buffer::Metadata.new(tag, time, record)
    chunk = driver.instance.buffer.generate_chunk(metadata).tap do |c|
      c.append([driver.instance.format(tag, time, record)])
    end

    stub_writer do |writer|
      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') { nil }

      mock(writer.client).insert_job('yourproject_id', {
        configuration: {
          load: {
            destination_table: {
              project_id: 'yourproject_id',
              dataset_id: 'yourdataset_id',
              table_id: 'foo',
            },
            write_disposition: "WRITE_APPEND",
            source_format: "NEWLINE_DELIMITED_JSON",
            ignore_unknown_values: false,
            max_bad_records: 0,
          }
        }
      }, upload_source: duck_type(:write, :sync, :rewind), content_type: "application/octet-stream") do
        stub!.job_reference.stub!.job_id { "dummy_job_id" }
      end

      mock(writer.client).get_job('yourproject_id', 'dummy_job_id', :location=>nil) do
        stub! do |s|
          s.id { 'dummy_job_id' }
          s.configuration.stub! do |_s|
            _s.load.stub! do |__s|
              __s.destination_table.stub! do |___s|
                ___s.project_id { 'yourproject_id' }
                ___s.dataset_id { 'yourdataset_id' }
                ___s.table_id { 'foo' }
              end
            end
          end
          s.status.stub! do |_s|
            _s.state { 'DONE' }
            _s.errors { [] }
            _s.error_result.stub! do |__s|
              __s.message { 'error' }
              __s.reason { 'backendError' }
            end
          end
        end
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

    driver.instance_start
    tag, time, record = "tag", Time.now.to_i, {"a" => "b"}
    metadata = Fluent::Plugin::Buffer::Metadata.new(tag, time, record)
    chunk = driver.instance.buffer.generate_chunk(metadata).tap do |c|
      c.append([driver.instance.format(tag, time, record)])
    end

    stub_writer do |writer|
      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') { nil }

      mock(writer.client).insert_job('yourproject_id', {
        configuration: {
          load: {
            destination_table: {
              project_id: 'yourproject_id',
              dataset_id: 'yourdataset_id',
              table_id: 'foo',
            },
            write_disposition: "WRITE_APPEND",
            source_format: "NEWLINE_DELIMITED_JSON",
            ignore_unknown_values: false,
            max_bad_records: 0,
          }
        }
      }, upload_source: duck_type(:write, :sync, :rewind), content_type: "application/octet-stream") do
        stub!.job_reference.stub!.job_id { "dummy_job_id" }
      end

      mock(writer.client).get_job('yourproject_id', 'dummy_job_id', :location=>nil) do
        stub! do |s|
          s.id { 'dummy_job_id' }
          s.configuration.stub! do |_s|
            _s.load.stub! do |__s|
              __s.destination_table.stub! do |___s|
                ___s.project_id { 'yourproject_id' }
                ___s.dataset_id { 'yourdataset_id' }
                ___s.table_id { 'foo' }
              end
            end
          end
          s.status.stub! do |_s|
            _s.state { 'DONE' }
            _s.errors { [] }
            _s.error_result.stub! do |__s|
              __s.message { 'error' }
              __s.reason { 'invalid' }
            end
          end
        end
      end
    end

    assert_raise Fluent::BigQuery::UnRetryableError do
      driver.instance.write(chunk)
    end
    assert_in_delta driver.instance.retry.secondary_transition_at , Time.now, 0.1
    driver.instance_shutdown
  end

  def test_write_with_auto_create_table
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

      auto_create_table true
      schema_path #{SCHEMA_PATH}
    CONFIG

    schema_fields = Fluent::BigQuery::Helper.deep_symbolize_keys(MultiJson.load(File.read(SCHEMA_PATH)))

    stub_writer do |writer|
      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') do
        raise Google::Apis::ClientError.new("notFound: Not found: Table yourproject_id:yourdataset_id.foo", status_code: 404)
      end

      mock(writer.client).insert_job('yourproject_id', {
        configuration: {
          load: {
            destination_table: {
              project_id: 'yourproject_id',
              dataset_id: 'yourdataset_id',
              table_id: 'foo',
            },
            write_disposition: "WRITE_APPEND",
            source_format: "NEWLINE_DELIMITED_JSON",
            ignore_unknown_values: false,
            max_bad_records: 0,
            schema: {
              fields: schema_fields,
            },
          }
        }
      }, upload_source: duck_type(:write, :sync, :rewind), content_type: "application/octet-stream") do
        stub!.job_reference.stub!.job_id { "dummy_job_id" }
      end
    end

    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end
  end

  private

  def create_response_stub(response)
    case response
    when Hash
      root = stub!
      response.each do |k, v|
        root.__send__(k) do
          create_response_stub(v)
        end
      end
      root
    when Array
      response.map { |item| create_response_stub(item) }
    else
      response
    end
  end
end
