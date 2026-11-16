using System;

namespace Network {
    public sealed class RestApiException : InvalidOperationException {
        public long StatusCode { get; }
        public string ResponseBody { get; }
        public string TransportError { get; }

        public RestApiException(string method, long statusCode, string error, string responseBody,
            Exception innerException = null)
            : base($"HTTP {method} failed (status={statusCode}): {error}", innerException) {
            StatusCode = statusCode;
            ResponseBody = responseBody;
            TransportError = error;
        }
    }
}
