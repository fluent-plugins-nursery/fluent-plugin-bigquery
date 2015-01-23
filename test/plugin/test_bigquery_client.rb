require 'helper'

class BigQueryClientTest < Test::Unit::TestCase
  def setup
    @client = Fluent::BigQueryPlugin::BigQueryClient.new(
      project:                '1234567890',
      dataset:                'my_dataset',
      email:                  '1234567890@developer.gserviceaccount.com',
      private_key_path:       '/path/to/keyfile.p12',
      private_key_passphrase: 'itsasecret',
      auth_method:            'private_key'
    )
  end

  def test_initialize
    actual   = @client.instance_variables
    expected = [:@project, :@dataset, :@email, :@private_key_path, :@private_key_passphrase, :@auth_method]
    assert { actual.sort == expected.sort }
  end

  def test_create_table
    params = {
      api_method: 'create_table',
      body_object: {
        'tableReference' => {
          'tableId' => 'my_table'
        },
        'schema' => {
          'fields' => [
            {'name' => 'foo', 'type' => 'timestamp'},
            {'name' => 'bar', 'type' => 'string'}
          ]
        }
      },
      parameters: {
        'projectId' => '1234567890',
        'datasetId' => 'my_dataset'
      }
    }
    auth = Object.new
    mock(client = Object.new) do |client|
      client.discovered_api('bigquery', 'v2') { mock!.tables.mock!.insert { 'create_table' } }
      client.execute(params) { mock(Object.new).error? { false } }
      client.authorization = auth
    end
    mock(Google::APIClient).new.with_any_args { client }
    mock(Google::APIClient::JWTAsserter).new.with_any_args { mock(Object.new).authorize { auth } }
    mock(Google::APIClient::PKCS12).load_key('/path/to/keyfile.p12', 'itsasecret')
    schema = [{'name' => 'foo', 'type' => 'timestamp'}, {'name' => 'bar', 'type' => 'string'}]
    assert { @client.create_table('my_table', schema).nil? }
  end

  def test_insert
    params = {
      api_method: 'insert_all',
      parameters: {
        'projectId' => '1234567890',
        'datasetId' => 'my_dataset',
        'tableId'   => 'my_table'
      },
      body_object: {
        'rows' => [
          { 'json' => { 'a' => 'b' } },
          { 'json' => { 'b' => 'c' } }
        ]
      }
    }
    auth = Object.new
    mock(client = Object.new) do |client|
      client.discovered_api('bigquery', 'v2') { mock!.tabledata.mock!.insert_all { 'insert_all' } }
      client.execute(params) { mock(Object.new).error? { false } }
      client.authorization = auth
    end
    mock(Google::APIClient).new.with_any_args { client }
    mock(Google::APIClient::JWTAsserter).new.with_any_args { mock(Object.new).authorize { auth } }
    mock(Google::APIClient::PKCS12).load_key('/path/to/keyfile.p12', 'itsasecret')
    rows = [{'json' => {'a' => 'b'}}, {'json' => {'b' => 'c'}}]
    assert { @client.insert('my_table', rows).nil? }
  end

  def test_fetch_schema
    params = {
      api_method: 'tables_get',
      parameters: {
        'tableId'   => 'my_table',
        'projectId' => '1234567890',
        'datasetId' => 'my_dataset'
      }
    }
    result_body = JSON.generate(
      {
        schema: {
          fields: [
            { name: 'time', type: 'TIMESTAMP' },
            { name: 'tty',  type: 'STRING'    }
          ]
        }
      }
    )
    auth = Object.new
    mock(result = Object.new) do |result|
      result.error? { false }
      result.body   { result_body }
    end
    mock(client = Object.new) do |client|
      client.discovered_api('bigquery', 'v2') { mock!.tables.mock!.get { 'tables_get' } }
      client.execute(params) { result }
      client.authorization = auth
    end
    mock(Google::APIClient).new.with_any_args { client }
    mock(Google::APIClient::JWTAsserter).new.with_any_args { mock(Object.new).authorize { auth } }
    mock(Google::APIClient::PKCS12).load_key('/path/to/keyfile.p12', 'itsasecret')
    expected = [
      { 'name' => 'time', 'type' => 'TIMESTAMP' },
      { 'name' => 'tty',  'type' => 'STRING'    }
    ]
    assert { @client.fetch_schema('my_table') == expected }
  end

  def test_errors
    errors = [
      { code: 404, klass: Fluent::BigQueryPlugin::NotFound        },
      { code: 409, klass: Fluent::BigQueryPlugin::Conflict        },
      { code: 403, klass: Fluent::BigQueryPlugin::ClientError     },
      { code: 503, klass: Fluent::BigQueryPlugin::ServerError     },
      { code: 301, klass: Fluent::BigQueryPlugin::UnexpectedError }
    ]
    errors.each do |error|
      auth = Object.new
      mock(result = Object.new) do |result|
        result.error?        { true }
        result.status        { error[:code] }
        result.error_message { 'this is an error message' }
      end
      mock(client = Object.new) do |client|
        client.execute.with_any_args { result }
        client.authorization = auth
      end
      mock(@client).bigquery { mock!.tabledata.mock!.insert_all }
      mock(Google::APIClient).new.with_any_args { client }
      mock(Google::APIClient::JWTAsserter).new.with_any_args { mock(Object.new).authorize { auth } }
      mock(Google::APIClient::PKCS12).load_key('/path/to/keyfile.p12', 'itsasecret')
      rows = [{'json' => {'a' => 'b'}}, {'json' => {'b' => 'c'}}]
      assert_raise(error[:klass]) { @client.insert('my_table', rows) }
    end
  end
end
