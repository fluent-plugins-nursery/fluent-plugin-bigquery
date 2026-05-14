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
        stub!.job_reference.stub! do |s|
          s.job_id { "dummy_job_id" }
          s.location { "us" }
        end
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
        stub!.job_reference.stub! do |s|
          s.job_id { "dummy_job_id" }
          s.location { "us" }
        end
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
        stub!.job_reference.stub! do |s|
          s.job_id { "dummy_job_id" }
          s.location { "us" }
        end
      end

      mock(writer.client).get_job('yourproject_id', 'dummy_job_id', location: "us") do
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
        stub!.job_reference.stub! do |s|
          s.job_id { "dummy_job_id" }
          s.location { "us" }
        end
      end

      mock(writer.client).get_job('yourproject_id', 'dummy_job_id', location: "us") do
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
        stub!.job_reference.stub! do |s|
          s.job_id { "dummy_job_id" }
          s.location { "us" }
        end
      end
    end

    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end
  end

  def test_write_with_autodetect_and_auto_create_table
    # GIVEN a bigquery_load output configured with autodetect=true and auto_create_table=true,
    # and a destination table that does not yet exist (BigQuery returns 404 on get_table)
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
      autodetect true
    CONFIG

    response_stub = stub!
    stub_writer do |writer|
      mock(writer).fetch_load_job(is_a(Fluent::BigQuery::Writer::JobReference)) { response_stub }
      mock(writer).commit_load_job(is_a(String), response_stub)

      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') do
        raise Google::Apis::ClientError.new("notFound: Not found: Table yourproject_id:yourdataset_id.foo", status_code: 404)
      end

      # THEN the load job carries `autodetect: true` and no `schema:` key — BigQuery infers the
      # schema and creates the destination table from the load file
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
            autodetect: true,
          }
        }
      }, upload_source: duck_type(:write, :sync, :rewind), content_type: "application/octet-stream") do
        stub!.job_reference.stub! do |s|
          s.job_id { "dummy_job_id" }
          s.location { "us" }
        end
      end
    end

    # WHEN a record is written
    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end
  end

  def test_write_with_autodetect
    # GIVEN a bigquery_load output configured with autodetect=true and no explicit schema source
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

      autodetect true
    CONFIG

    response_stub = stub!
    stub_writer do |writer|
      mock(writer).fetch_load_job(is_a(Fluent::BigQuery::Writer::JobReference)) { response_stub }
      mock(writer).commit_load_job(is_a(String), response_stub)

      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') { nil }

      # THEN the load job submitted to BigQuery has `autodetect: true` and no explicit `schema:` key
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
            autodetect: true,
          }
        }
      }, upload_source: duck_type(:write, :sync, :rewind), content_type: "application/octet-stream") do
        stub!.job_reference.stub! do |s|
          s.job_id { "dummy_job_id" }
          s.location { "us" }
        end
      end
    end

    # WHEN a record is written
    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end
  end

  def test_write_with_schema_update_options_and_schema_path
    # GIVEN a bigquery_load output configured with an explicit schema_path and ALLOW_FIELD_ADDITION
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
      schema_update_options ["ALLOW_FIELD_ADDITION"]
    CONFIG

    schema_fields = Fluent::BigQuery::Helper.deep_symbolize_keys(MultiJson.load(File.read(SCHEMA_PATH)))

    response_stub = stub!
    stub_writer do |writer|
      mock(writer).fetch_load_job(is_a(Fluent::BigQuery::Writer::JobReference)) { response_stub }
      mock(writer).commit_load_job(is_a(String), response_stub)

      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') { nil }

      # THEN the load job submitted to BigQuery carries the configured schema AND schema_update_options,
      # so BigQuery can compare the supplied schema against the destination table and add/relax columns
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
            schema_update_options: ["ALLOW_FIELD_ADDITION"],
            schema: {
              fields: schema_fields,
            },
          }
        }
      }, upload_source: duck_type(:write, :sync, :rewind), content_type: "application/octet-stream") do
        stub!.job_reference.stub! do |s|
          s.job_id { "dummy_job_id" }
          s.location { "us" }
        end
      end
    end

    # WHEN a record is written
    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end
  end

  def test_write_with_autodetect_and_schema_update_options
    # GIVEN a bigquery_load output configured with autodetect AND both schema_update_options values
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

      autodetect true
      schema_update_options ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"]
    CONFIG

    response_stub = stub!
    stub_writer do |writer|
      mock(writer).fetch_load_job(is_a(Fluent::BigQuery::Writer::JobReference)) { response_stub }
      mock(writer).commit_load_job(is_a(String), response_stub)

      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') { nil }

      # THEN the load job carries `autodetect: true` AND both schema_update_options, and no explicit `schema:`
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
            autodetect: true,
            schema_update_options: ["ALLOW_FIELD_ADDITION", "ALLOW_FIELD_RELAXATION"],
          }
        }
      }, upload_source: duck_type(:write, :sync, :rewind), content_type: "application/octet-stream") do
        stub!.job_reference.stub! do |s|
          s.job_id { "dummy_job_id" }
          s.location { "us" }
        end
      end
    end

    # WHEN a record is written
    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end
  end

  def test_configure_autodetect_conflicts_with_schema_path
    # GIVEN a config that combines autodetect with schema_path
    conf = <<-CONFIG
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <buffer>
        @type memory
      </buffer>

      autodetect true
      schema_path #{SCHEMA_PATH}
    CONFIG

    # WHEN the driver is configured
    # THEN configuration fails with a message naming schema_path as the conflicting schema source
    e = assert_raise(Fluent::ConfigError) { create_driver(conf) }
    assert_match(/'autodetect' cannot be combined with .*schema_path/, e.message)
  end

  def test_configure_autodetect_conflicts_with_fetch_schema
    # GIVEN a config that combines autodetect with fetch_schema
    conf = <<-CONFIG
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <buffer>
        @type memory
      </buffer>

      autodetect true
      fetch_schema true
    CONFIG

    # WHEN the driver is configured
    # THEN configuration fails with a message naming fetch_schema as the conflict
    e = assert_raise(Fluent::ConfigError) { create_driver(conf) }
    assert_match(/'autodetect' cannot be combined with .*fetch_schema/, e.message)
  end

  def test_configure_autodetect_conflicts_with_fetch_schema_table
    # GIVEN a config that combines autodetect with fetch_schema_table
    conf = <<-CONFIG
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <buffer>
        @type memory
      </buffer>

      autodetect true
      fetch_schema_table foo_schema
    CONFIG

    # WHEN the driver is configured
    # THEN configuration fails with a message naming fetch_schema_table as the conflict
    e = assert_raise(Fluent::ConfigError) { create_driver(conf) }
    assert_match(/'autodetect' cannot be combined with .*fetch_schema_table/, e.message)
  end

  def test_configure_autodetect_conflicts_with_inline_schema
    # GIVEN a config that combines autodetect with an inline schema
    conf = <<-CONFIG
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <buffer>
        @type memory
      </buffer>

      autodetect true
      schema [{"name":"time","type":"INTEGER"}]
    CONFIG

    # WHEN the driver is configured
    # THEN configuration fails with a message naming schema as the conflict
    e = assert_raise(Fluent::ConfigError) { create_driver(conf) }
    assert_match(/'autodetect' cannot be combined with .*\bschema\b/, e.message)
  end

  def test_configure_schema_update_options_rejects_unknown_value
    # GIVEN a config with a schema_update_options entry that isn't a valid BigQuery enum value
    conf = <<-CONFIG
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <buffer>
        @type memory
      </buffer>

      schema_path #{SCHEMA_PATH}
      schema_update_options ["ALLOW_FIELD_ADDITION", "BOGUS_OPTION"]
    CONFIG

    # WHEN the driver is configured
    # THEN configuration fails with a message that quotes the offending value
    e = assert_raise(Fluent::ConfigError) { create_driver(conf) }
    assert_match(/'schema_update_options' contains unknown value.*BOGUS_OPTION/, e.message)
  end

  def test_configure_schema_update_options_rejects_empty_array
    # GIVEN a config that sets schema_update_options to an empty array
    conf = <<-CONFIG
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <buffer>
        @type memory
      </buffer>

      schema_path #{SCHEMA_PATH}
      schema_update_options []
    CONFIG

    # WHEN the driver is configured
    # THEN configuration fails — an empty array would silently no-op rather than evolve the schema
    e = assert_raise(Fluent::ConfigError) { create_driver(conf) }
    assert_match(/'schema_update_options' must be a non-empty array/, e.message)
  end

  def test_configure_schema_update_options_requires_evolvable_schema
    # GIVEN a config that sets schema_update_options with only fetch_schema (no explicit/inferred schema source)
    conf = <<-CONFIG
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <buffer>
        @type memory
      </buffer>

      fetch_schema true
      schema_update_options ["ALLOW_FIELD_ADDITION"]
    CONFIG

    # WHEN the driver is configured
    # THEN configuration fails because fetch_schema mirrors the destination table and so can never
    # carry new columns for BigQuery to add (the option would silently no-op); the message
    # encodes that rationale so future maintainers don't loosen the rule by accident
    e = assert_raise(Fluent::ConfigError) { create_driver(conf) }
    assert_match(/'schema_update_options' requires 'autodetect', 'schema', or 'schema_path'/, e.message)
    assert_match(/no-op/, e.message)
  end

  def test_configure_schema_update_options_accepts_field_relaxation
    # GIVEN a config that pairs ALLOW_FIELD_RELAXATION with an explicit schema_path
    conf = <<-CONFIG
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <buffer>
        @type memory
      </buffer>

      schema_path #{SCHEMA_PATH}
      schema_update_options ["ALLOW_FIELD_RELAXATION"]
    CONFIG

    # WHEN the driver is configured
    # THEN configuration succeeds: ALLOW_FIELD_RELAXATION is a valid option and schema_path supplies an evolvable schema
    assert_nothing_raised { create_driver(conf) }
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
