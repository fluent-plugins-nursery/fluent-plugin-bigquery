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

  def test_write_with_prevent_duplicate_load_while_auto_creating_table
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
      auto_create_table true
      prevent_duplicate_load true
    CONFIG

    response_stub = stub!
    stub_writer do |writer|
      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') do
        raise Google::Apis::ClientError.new("notFound: Not found: Table yourproject_id:yourdataset_id.foo", status_code: 404)
      end
      mock(writer.client).insert_job.with_any_args do
        raise Google::Apis::ClientError.new("duplicate: Already Exists: Job yourproject_id:asia-northeast1.fluentd_job_x", status_code: 409)
      end
      mock(writer.client).get_dataset('yourproject_id', 'yourdataset_id') do
        stub!.location { "asia-northeast1" }
      end

      mock(writer).fetch_load_job(satisfy { |ref| ref.job_id =~ /\Afluentd_job_/ && ref.location == "asia-northeast1" }) { response_stub }
      mock(writer).commit_load_job(is_a(String), response_stub)
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

  def test_write_with_not_retryable_error_from_dataset_location
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
      auto_create_table true
      prevent_duplicate_load true
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
      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') do
        raise Google::Apis::ClientError.new("notFound: Not found: Table yourproject_id:yourdataset_id.foo", status_code: 404)
      end
      mock(writer.client).insert_job.with_any_args do
        raise Google::Apis::ClientError.new("duplicate: Already Exists: Job yourproject_id:asia-northeast1.fluentd_job_x", status_code: 409)
      end
      mock(writer.client).get_dataset('yourproject_id', 'yourdataset_id') do
        raise Google::Apis::ClientError.new("accessDenied: Access Denied: Dataset yourproject_id:yourdataset_id", status_code: 403)
      end
    end

    error = assert_raise Fluent::BigQuery::UnRetryableError do
      driver.instance.write(chunk)
    end
    assert_equal 403, error.status_code
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

  def test_write_with_auto_create_table_time_partitioning_expiration_in_seconds
    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <buffer>
        @type memory
      </buffer>

      auto_create_table true
      schema_path #{SCHEMA_PATH}

      time_partitioning_type day
      time_partitioning_expiration 2592000
    CONFIG

    job = nil
    response_stub = stub!
    stub_writer do |writer|
      mock(writer).fetch_load_job(is_a(Fluent::BigQuery::Writer::JobReference)) { response_stub }
      mock(writer).commit_load_job(is_a(String), response_stub)

      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') do
        raise Google::Apis::ClientError.new("notFound: Not found: Table yourproject_id:yourdataset_id.foo", status_code: 404)
      end

      mock(writer.client).insert_job('yourproject_id', anything, upload_source: duck_type(:write, :sync, :rewind), content_type: "application/octet-stream") do |_, configuration, _|
        job = configuration
        stub!.job_reference.stub! do |s|
          s.job_id { "dummy_job_id" }
          s.location { "us" }
        end
      end
    end

    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
    end

    time_partitioning = job[:configuration][:load][:time_partitioning]
    assert_equal({type: "DAY", expiration_ms: 2592000000}, time_partitioning)
    assert_kind_of(Integer, time_partitioning[:expiration_ms])
    json = Google::Apis::BigqueryV2::Job::Representation.new(job).to_json(user_options: { skip_undefined: true })
    assert_equal({"type" => "DAY", "expirationMs" => "2592000000"}, JSON.parse(json)["configuration"]["load"]["timePartitioning"])
  end

  def unretryable_error_queue_driver(use_delayed_commit:, retry_max_times: nil)
    driver = create_driver(<<-CONFIG)
      table foo_${tag}
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id
      use_delayed_commit #{use_delayed_commit}
      wait_job_interval 3600

      <buffer tag>
        @type memory
        disable_chunk_backup true
        #{"retry_max_times #{retry_max_times}" if retry_max_times}
      </buffer>

      <inject>
      time_format %s
      time_key  time
      </inject>

      schema_path #{SCHEMA_PATH}
    CONFIG

    driver.instance_start
    time = Time.now.to_i
    driver.instance.buffer.write({
      Fluent::Plugin::Buffer::Metadata.new(nil, "bad", nil) => [driver.instance.format("bad", time, {"a" => "b"})],
      Fluent::Plugin::Buffer::Metadata.new(nil, "good", nil) => [driver.instance.format("good", time, {"a" => "b"})],
    }, enqueue: true)
    assert_equal 2, driver.instance.buffer.queue.size
    driver
  end

  # Returns the tables passed to commit_load_job.
  def stub_load_jobs(writer, loaded_tables, fail_in: :create)
    committed_tables = []
    stub(writer).create_load_job do |chunk_id, chunk_id_hex, project, dataset, table_id, upload_source, schema|
      if table_id == "foo_bad" && fail_in == :create
        raise Fluent::BigQuery::UnRetryableError.new("invalid: Provided Schema does not match Table")
      end
      Fluent::BigQuery::Writer::JobReference.new(chunk_id, chunk_id_hex, project, dataset, table_id, "job_#{table_id}", "us")
    end
    stub(writer).fetch_load_job { |job_reference| job_reference }
    stub(writer).commit_load_job do |chunk_id_hex, job_reference|
      committed_tables << job_reference.table_id
      raise Fluent::BigQuery::UnRetryableError.new("failed to load into bigquery, and cannot retry") if job_reference.table_id == "foo_bad"
      loaded_tables << job_reference.table_id
    end
    committed_tables
  end

  def test_write_with_not_retryable_error_keeps_other_chunks
    driver = unretryable_error_queue_driver(use_delayed_commit: false)
    loaded_tables = []
    stub_load_jobs(driver.instance.writer, loaded_tables)

    driver.instance.try_flush
    driver.instance.try_flush

    assert_equal ["foo_good"], loaded_tables
    assert_equal 0, driver.instance.buffer.queue.size
    assert_nil driver.instance.retry
    assert_false driver.logs.any? { |log| log.include?("Hit limit for retries") }
  ensure
    driver&.instance_shutdown
  end

  def test_try_write_with_not_retryable_error_keeps_other_chunks
    driver = unretryable_error_queue_driver(use_delayed_commit: true)
    loaded_tables = []
    stub_load_jobs(driver.instance.writer, loaded_tables)

    driver.instance.try_flush
    driver.instance.try_flush
    driver.instance.send(:poll)

    assert_equal ["foo_good"], loaded_tables
    assert_equal 0, driver.instance.buffer.queue.size
    assert_equal 0, driver.instance.buffer.dequeued.size
    assert_nil driver.instance.retry
    assert_false driver.logs.any? { |log| log.include?("Hit limit for retries") }
  ensure
    driver&.instance_shutdown
  end

  def test_poll_with_not_retryable_error_keeps_other_chunks
    driver = unretryable_error_queue_driver(use_delayed_commit: true)
    loaded_tables = []
    committed_tables = stub_load_jobs(driver.instance.writer, loaded_tables, fail_in: :commit)

    driver.instance.try_flush
    driver.instance.send(:poll)
    driver.instance.try_flush
    driver.instance.try_flush
    driver.instance.send(:poll)

    assert_equal ["foo_good"], loaded_tables
    assert_include committed_tables, "foo_bad"
    assert_equal 0, driver.instance.buffer.queue.size
    assert_equal 0, driver.instance.buffer.dequeued.size
    assert_false driver.logs.any? { |log| log.include?("Hit limit for retries") }
  ensure
    driver&.instance_shutdown
  end

  def test_poll_with_not_retryable_error_does_not_update_retry
    driver = unretryable_error_queue_driver(use_delayed_commit: true, retry_max_times: 0)
    loaded_tables = []
    committed_tables = stub_load_jobs(driver.instance.writer, loaded_tables, fail_in: :commit)

    driver.instance.try_flush
    driver.instance.send(:poll)

    assert_equal ["foo_bad"], committed_tables
    assert_include driver.instance.buffer.queue.map { |chunk| chunk.metadata.tag }, "good"
    assert_nil driver.instance.retry

    driver.instance.try_flush
    driver.instance.try_flush
    driver.instance.send(:poll)

    assert_equal ["foo_good"], loaded_tables
    assert_equal 0, driver.instance.buffer.queue.size
    assert_equal 0, driver.instance.buffer.dequeued.size
    assert_false driver.logs.any? { |log| log.include?("Hit limit for retries") }
  ensure
    driver&.instance_shutdown
  end

  def test_configure_compression_default
    driver = create_driver
    assert_equal :none, driver.instance.compression
  end

  def test_configure_compression_invalid_level
    assert_raise(Fluent::ConfigError) do
      create_driver(CONFIG + "compression gzip\ncompression_level 10\n")
    end
  end

  def test_configure_compression_gzip_with_avro
    assert_raise(Fluent::ConfigError) do
      create_driver(CONFIG + "compression gzip\nsource_format avro\n")
    end
  end

  data(
    "memory buffer" => "",
    "memory buffer with compress gzip" => "compress gzip",
  )
  def test_write_with_compression_gzip(buffer_option)
    driver = create_driver(<<-CONFIG)
      table foo
      email foo@bar.example
      private_key_path /path/to/key
      project yourproject_id
      dataset yourdataset_id

      <buffer>
        @type memory
        #{buffer_option}
      </buffer>

      schema_path #{SCHEMA_PATH}
      wait_job_interval 0.1
      compression gzip
    CONFIG

    uploaded = nil
    response_stub = stub!
    stub_writer do |writer|
      mock(writer).fetch_load_job(is_a(Fluent::BigQuery::Writer::JobReference)) { response_stub }
      mock(writer).commit_load_job(is_a(String), response_stub)

      mock(writer.client).get_table('yourproject_id', 'yourdataset_id', 'foo') { nil }

      mock(writer.client).insert_job('yourproject_id', anything, upload_source: duck_type(:read, :rewind), content_type: "application/octet-stream") do |_, _, options|
        uploaded = options[:upload_source].read
        stub!.job_reference.stub! do |s|
          s.job_id { "dummy_job_id" }
          s.location { "us" }
        end
      end
    end

    driver.run do
      driver.feed("tag", Time.now.to_i, {"a" => "b"})
      driver.feed("tag", Time.now.to_i, {"a" => "c"})
    end

    # The upload source must be a single gzip stream of newline delimited JSON
    assert_equal "\x1f\x8b".b, uploaded.byteslice(0, 2)
    reader = Zlib::GzipReader.new(StringIO.new(uploaded))
    lines = reader.read.each_line.map { |line| JSON.parse(line) }
    assert_true reader.unused.nil?
    assert_equal [{"a" => "b"}, {"a" => "c"}], lines
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
