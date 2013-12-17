# -*- coding: utf-8 -*-

require 'fluent/mixin/config_placeholders'
require 'fluent/mixin/plaintextformatter'

module Fluent
  class BigQueryOutput < BufferedOutput
    Fluent::Plugin.register_output('bigquery', self)

    # https://developers.google.com/bigquery/browser-tool-quickstart
    # https://developers.google.com/bigquery/bigquery-api-quickstart


    # dataset_name
    #   The name can be up to 1,024 characters long, and consist of A-Z, a-z, 0-9, and the underscore,
    #   but it cannot start with a number or underscore, or have spaces.

    # table_id
    #   In Table ID, enter a name for your new table. Naming rules are the same as for your dataset.

    # see as simple reference
    #   https://github.com/abronte/BigQuery/blob/master/lib/bigquery.rb

    # https://developers.google.com/bigquery/loading-data-into-bigquery
    # Maximum File Sizes:
    # File Type   Compressed   Uncompressed
    # CSV         1 GB         With new-lines in strings: 4 GB
    #                          Without new-lines in strings: 1 TB
    # JSON        1 GB         1 TB
    config_set_default :buffer_type, 'memory'
    config_set_default :flush_interval, 1800 # 30min => 48 imports/day
    config_set_default :buffer_chunk_limit, 1000*1000*1000*1000 # 1.0*10^12 < 1TB (1024^4)

    ### OAuth credential
    # config_param :client_id, :string
    # config_param :client_secret, :string

    ### Service Account credential
    config_param :email, :string
    config_param :private_key_path, :string
    config_param :private_key_passphrase, :string, :default => 'notasecret'

    config_param :project_id, :string

    def initialize
      require 'google/api_client'
      require 'google/api_client/client_secrets'
      require 'google/api_client/auth/installed_app'
    end

    def client
      @bq = client.discovered_api("bigquery", "v2")

      client = Google::APIClient.new
      key = Google::APIClient::PKCS12.load_key( @private_key_path, @private_key_passphrase )
      asserter = Google::APIClient::JWTAsserter.new(
        @email,
        "https://www.googleapis.com/auth/bigquery",
        key
      )
      # refresh_auth
      client.authorization = asserter.authorize
      client
    end

    def client_oauth # not implemented
      raise NotImplementedError, "OAuth needs browser authentication..."

      client = Google::APIClient.new(
        :application_name => 'Example Ruby application',
        :application_version => '1.0.0'
      )
      bigquery = client.discovered_api('bigquery', 'v2')
      flow = Google::APIClient::InstalledAppFlow.new(
        :client_id => @client_id
        :client_secret => @client_secret
        :scope => ['https://www.googleapis.com/auth/bigquery']
      )
      client.authorization = flow.authorize # browser authentication !
      client
    end

    def get_datasets_list
      client().execute(@bq.datasets.list)
    end

    def execute(method)
      client().execute( :api_method => method, :parameters => {'projectId' => @project_id} )
    end
  end
end
