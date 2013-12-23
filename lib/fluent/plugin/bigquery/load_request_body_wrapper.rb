module Fluent
  module BigQueryPlugin
    class LoadRequestBodyWrapper
      # body can be a instance of IO (#rewind, #read, #to_str)
      #   http://rubydoc.info/github/google/google-api-ruby-client/Google/APIClient/Request#body-instance_method

      # http://rubydoc.info/github/google/google-api-ruby-client/Google/APIClient#execute-instance_method
      # (Google::APIClient::Method) api_method: The method object or the RPC name of the method being executed.
      # (Hash, Array) parameters: The parameters to send to the method.
      # (String) body: The body of the request.
      # (Hash, Array) headers: The HTTP headers for the request.
      # (Hash) options: A set of options for the request, of which:
      #          (#generate_authenticated_request) :authorization (default: true)
      #                       - The authorization mechanism for the response. Used only if :authenticated is true.
      #          (TrueClass, FalseClass) :authenticated (default: true)
      #                       - true if the request must be signed or somehow authenticated, false otherwise.
      #          (TrueClass, FalseClass) :gzip (default: true) - true if gzip enabled, false otherwise.

      # https://developers.google.com/bigquery/loading-data-into-bigquery#loaddatapostrequest

      JSON_PRETTY_DUMP = JSON::State.new(space: " ", indent:"  ", object_nl:"\n", array_nl:"\n")

      CONTENT_TYPE_FIRST = "Content-Type: application/json; charset=UTF-8\n\n"
      CONTENT_TYPE_SECOND = "Content-Type: application/octet-stream\n\n"

      MULTIPART_BOUNDARY = "--xxx\n"
      MULTIPART_BOUNDARY_END = "--xxx--\n"

      def initialize(project_id, dataset_id, table_id, field_defs, buffer)
        @metadata = {
          configuration: {
            load: {
              sourceFormat: "<required for JSON files>",
              schema: {
                fields: field_defs
              },
              destinationTable: {
                projectId: project_id,
                datasetId: dataset_id,
                tableId: table_id
              }
            }
          }
        }

        @non_buffer = MULTIPART_BOUNDARY + CONTENT_TYPE_FIRST + @metadata.to_json(JSON_PRETTY_DUMP) + "\n" +
          MULTIPART_BOUNDARY + CONTENT_TYPE_SECOND
        @non_buffer.force_encoding("ASCII-8BIT")
        @non_buffer_bytesize = @non_buffer.bytesize

        @buffer = buffer # read
        @buffer_bytesize = @buffer.size # Fluentd Buffer Chunk #size -> bytesize

        @footer = MULTIPART_BOUNDARY_END.force_encoding("ASCII-8BIT")

        @contents_bytesize = @non_buffer_bytesize + @buffer_bytesize
        @total_bytesize = @contents_bytesize + MULTIPART_BOUNDARY_END.bytesize

        @whole_data = nil

        @counter = 0
        @eof = false
      end

#       sample_body = <<EOF
# --xxx
# Content-Type: application/json; charset=UTF-8
# 
# {
#   "configuration": {
#     "load": {
#       "sourceFormat": "<required for JSON files>",
#       "schema": {
#         "fields": [
#           {"name":"f1", "type":"STRING"},
#           {"name":"f2", "type":"INTEGER"}
#         ]
#       },
#       "destinationTable": {
#         "projectId": "projectId",
#         "datasetId": "datasetId",
#         "tableId": "tableId"
#       }
#     }
#   }
# }
# --xxx
# Content-Type: application/octet-stream
# 
# <your data>
# --xxx--
# EOF
      def rewind
        @counter = 0
        @eof = false
      end

      def eof?
        @eof
      end

      def to_str
        rewind
        self.read # all data
      end

      def read(length=nil, outbuf="")
        raise ArgumentError, "negative read length" if length && length < 0
        return (length.nil? || length == 0) ? "" : nil if @eof
        return outbuf if length == 0

        # read all data
        if length.nil? || length >= @total_bytesize
          @whole_data ||= @buffer.read.force_encoding("ASCII-8BIT")

          if @counter.zero?
            outbuf.replace(@non_buffer)
            outbuf << @whole_data
            outbuf << @footer
          elsif @counter < @non_buffer_bytesize
            outbuf.replace(@non_buffer[ @counter .. -1 ])
            outbuf << @whole_data
            outbuf << @footer
          elsif @counter < @contents_bytesize
            outbuf.replace(@whole_data[ (@counter - @non_buffer_bytesize) .. -1 ])
            outbuf << @footer
          else
            outbuf.replace(@footer[ (@counter - @contents_bytesize) .. -1 ])
          end
          @counter = @total_bytesize
          @eof = true
          return outbuf
        end

        # In ruby script level (non-ext module), we cannot prevent to change outbuf length or object re-assignment
        outbuf.replace("")

        # return first part (metadata)
        if @counter < @non_buffer_bytesize
          non_buffer_part = @non_buffer[@counter, length]
          if non_buffer_part
            outbuf << non_buffer_part
            length -= non_buffer_part.bytesize
            @counter += non_buffer_part.bytesize
          end
        end
        return outbuf if length < 1

        # return second part (buffer content)
        if @counter < @contents_bytesize
          @whole_data ||= @buffer.read.force_encoding("ASCII-8BIT")
          buffer_part = @whole_data[@counter - @non_buffer_bytesize, length]
          if buffer_part
            outbuf << buffer_part
            length -= buffer_part.bytesize
            @counter += buffer_part.bytesize
          end
        end
        return outbuf if length < 1

        # return footer
        footer_part = @footer[@counter - @contents_bytesize, length]
        if footer_part
          outbuf << footer_part
          @counter += footer_part.bytesize
          @eof = true if @counter >= @total_bytesize
        end

        outbuf
      end
    end
  end
end
