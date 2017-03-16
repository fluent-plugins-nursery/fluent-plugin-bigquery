module Fluent
  module BigQuery
    # @abstract
    class Error < StandardError
      RETRYABLE_ERROR_REASON = %w(backendError internalError rateLimitExceeded tableUnavailable).freeze
      RETRYABLE_INSERT_ERRORS_REASON = %w(timeout).freeze
      RETRYABLE_STATUS_CODE = [500, 502, 503, 504]

      class << self
        def wrap(google_api_error, message = nil, force_unretryable: false)
          e = google_api_error
          return UnRetryableError.new(message, e) if force_unretryable

          if retryable_error?(e)
            RetryableError.new(message, e)
          else
            UnRetryableError.new(message, e)
          end
        end

        def retryable_error?(google_api_error)
          e = google_api_error
          reason = e.respond_to?(:reason) ? e.reason : nil

          retryable_error_reason?(reason) ||
            (e.is_a?(Google::Apis::ServerError) && RETRYABLE_STATUS_CODE.include?(e.status_code))
        end

        def retryable_error_reason?(reason)
          RETRYABLE_ERROR_REASON.include?(reason)
        end

        def retryable_insert_errors_reason?(reason)
          RETRYABLE_INSERT_ERRORS_REASON.include?(reason)
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
