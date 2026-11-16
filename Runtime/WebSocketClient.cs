using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Cysharp.Threading.Tasks;
using NativeWebSocket;
using Newtonsoft.Json;

namespace Network {
    /// <summary>One connection session. Create a new client for each reconnect.</summary>
    public sealed class WebSocketClient : IWebSocketClient {
        private readonly string url;
        private readonly TimeSpan closeTimeout;
        private readonly int maxPendingMessages;
        private readonly Queue<string> pendingMessages = new();
        private readonly SemaphoreSlim sendGate = new(1, 1);
        private readonly UniTaskCompletionSource opened = new();
        private WebSocket socket;
        private bool connectionStarted;
        private bool waitingForOpen;

        public event Action Opened;
        public event Action<string> MessageReceived;
        public event Action<string> ErrorReceived;
        public event Action<WebSocketCloseCode> Closed;
        public bool IsConnected => socket?.State == WebSocketState.Open;

        public WebSocketClient(string url, TimeSpan? closeTimeout = null, int maxPendingMessages = 256) {
            if (!Uri.TryCreate(url, UriKind.Absolute, out var uri) || (uri.Scheme != "ws" && uri.Scheme != "wss")) {
                throw new ArgumentException("An absolute ws:// or wss:// URL is required.", nameof(url));
            }
            if (maxPendingMessages < 1) throw new ArgumentOutOfRangeException(nameof(maxPendingMessages));
            this.closeTimeout = closeTimeout ?? TimeSpan.FromSeconds(3);
            if (this.closeTimeout <= TimeSpan.Zero) throw new ArgumentOutOfRangeException(nameof(closeTimeout));
            this.url = url;
            this.maxPendingMessages = maxPendingMessages;
        }

        public void Connect(string jwtToken = null) {
            if (connectionStarted) throw new InvalidOperationException("This client has already started a connection.");
            connectionStarted = true;
            var headers = string.IsNullOrEmpty(jwtToken) ? null :
                new Dictionary<string, string> { { "Authorization", "Bearer " + jwtToken } };
            socket = new WebSocket(url, headers);
            socket.OnOpen += HandleOpen;
            socket.OnMessage += HandleMessage;
            socket.OnError += HandleError;
            socket.OnClose += HandleClose;
            RunConnectionAsync(socket).Forget();
        }

        public async UniTask ConnectAsync(string jwtToken = null, CancellationToken cancellationToken = default) {
            cancellationToken.ThrowIfCancellationRequested();
            waitingForOpen = true;
            Connect(jwtToken);
            try {
                // NativeWebSocket.Connect completes at disconnect on native platforms.
                // Await the actual OnOpen event instead of that transport task.
                await opened.Task.AttachExternalCancellation(cancellationToken);
            } catch {
                Abort();
                throw;
            }
        }

        public async UniTask SendStringAsync(string message) {
            if (message == null) throw new ArgumentNullException(nameof(message));
            var target = socket;
            if (target == null) throw new InvalidOperationException("WebSocket is not connected.");
            // Native State can turn Open before Unity dispatches OnOpen. Keep
            // queuing until that dispatch so new messages cannot overtake old ones.
            if (!opened.Task.Status.IsCompleted()) {
                if (pendingMessages.Count >= maxPendingMessages) throw new InvalidOperationException("Pending WebSocket message queue is full.");
                pendingMessages.Enqueue(message);
                return;
            }
            await sendGate.WaitAsync();
            try {
                if (!ReferenceEquals(socket, target) || target.State != WebSocketState.Open) {
                    throw new InvalidOperationException("WebSocket is not connected.");
                }
                await target.SendText(message);
            } finally {
                sendGate.Release();
            }
        }

        public UniTask SendJsonAsync<T>(T message) => SendStringAsync(JsonConvert.SerializeObject(message));

        public async UniTask DisconnectAsync() {
            var target = Detach();
            if (target == null) return;
            var closeCode = WebSocketCloseCode.Abnormal;
            try {
                if (target.State == WebSocketState.Open) {
                    var close = target.Close();
                    if (await Task.WhenAny(close, Task.Delay(closeTimeout)) == close) {
                        await close;
                        closeCode = WebSocketCloseCode.Normal;
                    } else {
                        ObserveLateCloseAsync(close).Forget();
                    }
                }
            } catch (Exception) {
                // Regardless of a failed close handshake, terminate the local transport.
            } finally {
                CancelTransport(target);
                Closed?.Invoke(closeCode);
            }
        }

        public void Abort() {
            var target = Detach();
            if (target == null) return;
            CancelTransport(target);
            Closed?.Invoke(WebSocketCloseCode.Abnormal);
        }

        private WebSocket Detach() {
            var target = socket;
            if (target == null) return null;
            socket = null;
            UnregisterEvents(target);
            pendingMessages.Clear();
            if (waitingForOpen) opened.TrySetException(new InvalidOperationException("WebSocket closed before opening."));
            else opened.TrySetCanceled();
            return target;
        }

        private static void CancelTransport(WebSocket target) {
#if UNITY_WEBGL && !UNITY_EDITOR
            // NativeWebSocket's browser CancelConnection only acts on Open sockets.
            // Browser close(1000) also terminates an unfinished handshake.
            CloseBrowserSocketAsync(target).Forget();
#else
            target.CancelConnection();
#endif
        }

#if UNITY_WEBGL && !UNITY_EDITOR
        private static async UniTaskVoid CloseBrowserSocketAsync(WebSocket target) {
            try { await target.Close(WebSocketCloseCode.Normal); }
            catch (Exception) { /* The browser may already have disposed the socket. */ }
        }
#endif

        private async UniTaskVoid RunConnectionAsync(WebSocket target) {
            try {
                await target.Connect();
            } catch (Exception exception) {
                if (!ReferenceEquals(socket, target)) return;
                // Never propagate the transport's URL/query or credentials into logs.
                FailConnection($"WebSocket connection failed ({exception.GetType().Name}).");
                HandleClose(WebSocketCloseCode.Abnormal);
            }
        }

        private static async UniTaskVoid ObserveLateCloseAsync(Task close) {
            try { await close; } catch (Exception) { /* Already closed locally. */ }
        }

        private async UniTaskVoid FlushPendingAsync(WebSocket target) {
            await sendGate.WaitAsync();
            try {
                while (pendingMessages.Count > 0 && ReferenceEquals(socket, target) && IsConnected) {
                    await target.SendText(pendingMessages.Dequeue());
                }
            } catch (Exception exception) {
                if (ReferenceEquals(socket, target)) FailConnection($"WebSocket send failed ({exception.GetType().Name}).");
            } finally {
                sendGate.Release();
            }
        }

        private void UnregisterEvents(WebSocket target) {
            target.OnOpen -= HandleOpen;
            target.OnMessage -= HandleMessage;
            target.OnError -= HandleError;
            target.OnClose -= HandleClose;
        }

        private void HandleOpen() {
            if (socket == null) return;
            FlushPendingAsync(socket).Forget();
            opened.TrySetResult();
            Opened?.Invoke();
        }

        private void HandleMessage(byte[] bytes) {
            if (socket != null) MessageReceived?.Invoke(Encoding.UTF8.GetString(bytes));
        }

        private void HandleError(string ignoredTransportMessage) {
            if (socket != null) FailConnection("WebSocket transport error.");
        }

        private void FailConnection(string error) {
            if (waitingForOpen) opened.TrySetException(new InvalidOperationException(error));
            else opened.TrySetCanceled();
            ErrorReceived?.Invoke(error);
        }

        private void HandleClose(WebSocketCloseCode code) {
            if (Detach() != null) Closed?.Invoke(code);
        }
    }
}
