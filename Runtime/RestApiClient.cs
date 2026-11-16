using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using Cysharp.Threading.Tasks;
using Newtonsoft.Json;
using UnityEngine.Networking;

namespace Network {
    public sealed class RestApiClient {
        private readonly string baseUrl;
        private readonly Dictionary<string, string> headers = new(StringComparer.OrdinalIgnoreCase);
        private readonly JsonSerializerSettings jsonSettings;
        public int TimeoutSeconds { get; }

        public RestApiClient(string baseUrl, int timeoutSeconds = 30, JsonSerializerSettings jsonSettings = null) {
            if (!Uri.TryCreate(baseUrl, UriKind.Absolute, out var uri) ||
                (uri.Scheme != Uri.UriSchemeHttp && uri.Scheme != Uri.UriSchemeHttps) ||
                !string.IsNullOrEmpty(uri.Query) || !string.IsNullOrEmpty(uri.Fragment)) {
                throw new ArgumentException("An absolute HTTP(S) base URL without query or fragment is required.", nameof(baseUrl));
            }
            if (timeoutSeconds < 0) throw new ArgumentOutOfRangeException(nameof(timeoutSeconds));
            this.baseUrl = baseUrl.TrimEnd('/');
            TimeoutSeconds = timeoutSeconds;
            this.jsonSettings = jsonSettings;
        }

        public void SetJwtToken(string accessToken) {
            if (string.IsNullOrEmpty(accessToken)) headers.Remove("Authorization");
            else SetHeader("Authorization", "Bearer " + accessToken);
        }

        public void SetHeader(string name, string value) {
            if (string.IsNullOrWhiteSpace(name)) throw new ArgumentException("Header name is required.", nameof(name));
            if (value == null) throw new ArgumentNullException(nameof(value));
            headers[name] = value;
        }

        public bool RemoveHeader(string name) => headers.Remove(name);

        public UniTask<TResponse> GetAsync<TResponse>(string path, CancellationToken cancellationToken = default) =>
            SendAsync<object, TResponse>("GET", path, null, cancellationToken);
        public UniTask<TResponse> PostAsync<TRequest, TResponse>(string path, TRequest body, CancellationToken cancellationToken = default) =>
            SendAsync<TRequest, TResponse>("POST", path, body, cancellationToken);
        public UniTask<TResponse> PutAsync<TRequest, TResponse>(string path, TRequest body, CancellationToken cancellationToken = default) =>
            SendAsync<TRequest, TResponse>("PUT", path, body, cancellationToken);
        public UniTask<TResponse> PatchAsync<TRequest, TResponse>(string path, TRequest body, CancellationToken cancellationToken = default) =>
            SendAsync<TRequest, TResponse>("PATCH", path, body, cancellationToken);
        public async UniTask DeleteAsync(string path, CancellationToken cancellationToken = default) =>
            await SendRawAsync("DELETE", path, null, cancellationToken);

        private async UniTask<TResponse> SendAsync<TRequest, TResponse>(string method, string path, TRequest body, CancellationToken cancellationToken) {
            string json = body == null ? null : JsonConvert.SerializeObject(body, jsonSettings);
            string response = await SendRawAsync(method, path, json, cancellationToken);
            return string.IsNullOrWhiteSpace(response) ? default : JsonConvert.DeserializeObject<TResponse>(response, jsonSettings);
        }

        private async UniTask<string> SendRawAsync(string method, string path, string body, CancellationToken cancellationToken) {
            if (path == null) throw new ArgumentNullException(nameof(path));
            if (path.Contains("://")) throw new ArgumentException("Use a relative request path.", nameof(path));
            cancellationToken.ThrowIfCancellationRequested();
            using var request = new UnityWebRequest(baseUrl + "/" + path.TrimStart('/'), method) {
                downloadHandler = new DownloadHandlerBuffer(),
                timeout = TimeoutSeconds
            };
            request.SetRequestHeader("Accept", "application/json");
            if (body != null) {
                request.uploadHandler = new UploadHandlerRaw(Encoding.UTF8.GetBytes(body));
                request.SetRequestHeader("Content-Type", "application/json");
            }
            foreach (var header in headers) request.SetRequestHeader(header.Key, header.Value);
            try {
                await request.SendWebRequest().WithCancellation(cancellationToken, cancelImmediately: true);
            } catch (UnityWebRequestException exception) {
                // UniTask throws on HTTP errors before an explicit result check can run.
                throw new RestApiException(method, request.responseCode, request.error,
                    request.downloadHandler?.text, exception);
            }
            return request.downloadHandler?.text;
        }
    }
}
