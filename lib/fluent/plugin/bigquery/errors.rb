module Fluent
  module BigQuery
    # @abstract
    class Error < StandardError
      RETRYABLE_ERROR_REASON = %w(backendError internalError rateLimitExceeded tableUnavailable).freeze
      RETRYABLE_INSERT_ERRORS_REASON = %w(timeout backendError internalError rateLimitExceeded).freeze
      RETRYABLE_STATUS_CODE = [500, 502, 503, 504]
      REGION_NOT_WRITABLE_MESSAGE = -"is not writable in the region"

      class << self
        # @param e [Google::Apis::Error]
        # @param message [String]
        def wrap(e, message = nil)
          if retryable_error?(e)
            RetryableError.new(message, e)
          else
            UnRetryableError.new(message, e)
          end
        end

        # @param e [Google::Apis::Error]
        def retryable_error?(e)
          retryable_server_error?(e) || retryable_region_not_writable?(e)
        end

        def retryable_server_error?(e)
          e.is_a?(Google::Apis::ServerError) && RETRYABLE_STATUS_CODE.include?(e.status_code)
        end

        def retryable_error_reason?(reason)
          RETRYABLE_ERROR_REASON.include?(reason)
        end

        def retryable_insert_errors_reason?(reason)
          RETRYABLE_INSERT_ERRORS_REASON.include?(reason)
        end

        def retryable_region_not_writable?(e)
          e.is_a?(Google::Apis::ClientError) && e.status_code == 400 && e.message.include?(REGION_NOT_WRITABLE_MESSAGE)
        end

        # Guard for instantiation
        private :new
        def inherited(subclass)
          subclass.class_eval do
            class << self
              public :new
            end
          end
        end
      end

      attr_reader :origin

      def initialize(message, origin = nil)
        @origin = origin
        super(message || origin.message)
      end

      def method_missing(name, *args)
        if @origin
          @origin.send(name, *args)
        else
          super
        end
      end

      def reason
        @origin && @origin.respond_to?(:reason) ? @origin.reason : nil
      end

      def status_code
        @origin && @origin.respond_to?(:status_code) ? @origin.status_code : nil
      end

      def body
        @origin && @origin.respond_to?(:body) ? @origin.body : nil
      end

      def retryable?
        false
      end
    end

    class UnRetryableError < Error; end

    class RetryableError < Error
      def retryable?
        true
      end
    end
  end
end
