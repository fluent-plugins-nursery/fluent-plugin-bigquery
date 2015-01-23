module Fluent::BigQueryPlugin
  class BigQueryAPIError < StandardError
  end

  # HTTP 4xx Client Error
  class ClientError < BigQueryAPIError
  end

  # HTTP 404 Not Found
  class NotFound < ClientError
  end

  # HTTP 409 Conflict
  class Conflict < ClientError
  end

  # HTTP 5xx Server Error
  class ServerError < BigQueryAPIError
  end

  class UnexpectedError < BigQueryAPIError
  end
end
