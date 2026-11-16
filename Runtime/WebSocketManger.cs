using System;
using System.Threading;
using Cysharp.Threading.Tasks;
using NativeWebSocket;
using UnityEngine;

namespace Network {
    // The requested public name is WebSocketManger (without the second 'a').
    [DefaultExecutionOrder(-200)]
    public class WebSocketManger : MonoBehaviour {
        private sealed class Connection {
            public IWebSocketClient Client;
            public Action Opened;
            public Action<string> Message;
            public Action<string> Error;
            public Action<WebSocketCloseCode> Closed;
        }

        private readonly SemaphoreSlim connectionGate = new(1, 1);
        private readonly CancellationTokenSource lifetime = new();
        private CancellationTokenSource pendingConnection;
        private Connection current;
        private string jwtAccessToken;
        private bool destroyed;

        public bool IsConnected => current?.Client.IsConnected == true;
        protected IWebSocketClient SocketClient => current?.Client;
        public event Action SocketOpened;
        public event Action<string> MessageReceived;
        public event Action<string> SocketErrorReceived;
        /// <summary>Raised on a remote close; explicit disconnect/replacement is not unexpected.</summary>
        public event Action SocketDisconnected;

        public void SetJwtToken(string accessToken) => jwtAccessToken = accessToken;

        public async UniTask ConnectSocketAsync(string url, CancellationToken cancellationToken = default) {
            ThrowIfDestroyed();
            cancellationToken.ThrowIfCancellationRequested();
            // A new connect replaces an unfinished handshake as well as an open socket.
            pendingConnection?.Cancel();
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, lifetime.Token);
            await connectionGate.WaitAsync(linked.Token);
            Connection connection = null;
            try {
                ThrowIfDestroyed();
                pendingConnection = linked;
                await DisconnectCurrentAsync();
                linked.Token.ThrowIfCancellationRequested();
                var client = CreateSocketClient(url) ?? throw new InvalidOperationException("CreateSocketClient returned null.");
                connection = CreateConnection(client);
                current = connection;
                Subscribe(connection);
                await client.ConnectAsync(jwtAccessToken, linked.Token);
                linked.Token.ThrowIfCancellationRequested();
                if (!ReferenceEquals(current, connection) || !client.IsConnected) {
                    throw new InvalidOperationException("WebSocket closed while connecting.");
                }
            } catch {
                if (connection != null) {
                    if (ReferenceEquals(current, connection)) {
                        current = null;
                        Unsubscribe(connection);
                    }
                    connection.Client.Abort();
                }
                throw;
            } finally {
                if (ReferenceEquals(pendingConnection, linked)) pendingConnection = null;
                connectionGate.Release();
            }
        }

        public async UniTask DisconnectSocketAsync() {
            ThrowIfDestroyed();
            pendingConnection?.Cancel();
            await connectionGate.WaitAsync(lifetime.Token);
            try { await DisconnectCurrentAsync(); }
            finally { connectionGate.Release(); }
        }

        public UniTask SendSocketJsonAsync<T>(T message) {
            ThrowIfDestroyed();
            if (message == null) throw new ArgumentNullException(nameof(message));
            var client = SocketClient ?? throw new InvalidOperationException("WebSocket is not created.");
            return client.SendJsonAsync(message);
        }

        public void AbortSocket() {
            pendingConnection?.Cancel();
            var connection = current;
            if (connection == null) return;
            current = null;
            Unsubscribe(connection);
            connection.Client.Abort();
        }

        protected virtual IWebSocketClient CreateSocketClient(string url) => new WebSocketClient(url);

        // Logging is independent of core message routing and connection cleanup.
        protected virtual void RegisterSocketLogEvents(IWebSocketClient client) {
            client.Opened += LogSocketOpened;
            client.ErrorReceived += LogSocketError;
            client.Closed += LogSocketClosed;
        }

        protected virtual void UnregisterSocketLogEvents(IWebSocketClient client) {
            client.Opened -= LogSocketOpened;
            client.ErrorReceived -= LogSocketError;
            client.Closed -= LogSocketClosed;
        }

        protected virtual void LogSocketOpened() => Debug.Log("WebSocket opened.");
        protected virtual void LogSocketError(string error) => Debug.LogError(error);
        protected virtual void LogSocketClosed(WebSocketCloseCode code) => Debug.Log($"WebSocket closed: {code}");

        /// <summary>Override to parse your project's JSON/protocol. No DTO is assumed.</summary>
        protected virtual void HandleSocketMessage(string message) => MessageReceived?.Invoke(message);

        protected virtual void OnApplicationQuit() => AbortSocket();
        protected virtual void OnDestroy() {
            destroyed = true;
            lifetime.Cancel();
            AbortSocket();
            lifetime.Dispose();
        }

        private Connection CreateConnection(IWebSocketClient client) {
            var connection = new Connection { Client = client };
            connection.Opened = () => { if (ReferenceEquals(current, connection)) SocketOpened?.Invoke(); };
            connection.Message = message => { if (ReferenceEquals(current, connection)) HandleSocketMessage(message); };
            connection.Error = error => { if (ReferenceEquals(current, connection)) SocketErrorReceived?.Invoke(error); };
            connection.Closed = code => {
                if (!ReferenceEquals(current, connection)) return;
                current = null;
                Unsubscribe(connection);
                SocketDisconnected?.Invoke();
            };
            return connection;
        }

        private void Subscribe(Connection connection) {
            var client = connection.Client;
            // Register logs first so remote-close cleanup cannot suppress a close log.
            RegisterSocketLogEvents(client);
            client.Opened += connection.Opened;
            client.MessageReceived += connection.Message;
            client.ErrorReceived += connection.Error;
            client.Closed += connection.Closed;
        }

        private void Unsubscribe(Connection connection) {
            var client = connection.Client;
            client.Opened -= connection.Opened;
            client.MessageReceived -= connection.Message;
            client.ErrorReceived -= connection.Error;
            client.Closed -= connection.Closed;
            UnregisterSocketLogEvents(client);
        }

        private async UniTask DisconnectCurrentAsync() {
            var connection = current;
            if (connection == null) return;
            current = null;
            Unsubscribe(connection);
            await connection.Client.DisconnectAsync();
        }

        private void ThrowIfDestroyed() {
            if (destroyed) throw new ObjectDisposedException(nameof(WebSocketManger));
        }
    }
}
