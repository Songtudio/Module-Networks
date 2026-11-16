using System;
using System.Collections.Generic;
using System.Threading;
using Cysharp.Threading.Tasks;
using NativeWebSocket;
using Network;
using Newtonsoft.Json;

namespace Song.UnityNetworks.Tests {
    public class TestWebSocketManger : WebSocketManger {
        public readonly Queue<FakeSocketClient> Clients = new();
        public int Registrations, Unregistrations, Opens, Errors, Closes;
        public bool CustomRegistration;
        public string LastHandledMessage;
        protected override IWebSocketClient CreateSocketClient(string url) => Clients.Dequeue();
        protected override void HandleSocketMessage(string message) {
            LastHandledMessage = message;
            base.HandleSocketMessage(message);
        }
        protected override void RegisterSocketLogEvents(IWebSocketClient client) {
            Registrations++;
            if (CustomRegistration) client.Opened += CustomOpen;
            else base.RegisterSocketLogEvents(client);
        }
        protected override void UnregisterSocketLogEvents(IWebSocketClient client) {
            Unregistrations++;
            if (CustomRegistration) client.Opened -= CustomOpen;
            else base.UnregisterSocketLogEvents(client);
        }
        private void CustomOpen() => Opens++;
        protected override void LogSocketOpened() => Opens++;
        protected override void LogSocketError(string error) => Errors++;
        protected override void LogSocketClosed(WebSocketCloseCode code) => Closes++;
    }

    public sealed class FakeSocketClient : IWebSocketClient {
        public event Action Opened;
        public event Action<string> MessageReceived;
        public event Action<string> ErrorReceived;
        public event Action<WebSocketCloseCode> Closed;
        public bool IsConnected { get; private set; }
        public bool DelayOpen, FailOpen;
        public int Disconnects, Aborts;
        public string JwtToken, LastJson;
        private readonly UniTaskCompletionSource opened = new();
        public int SubscriberCount => (Opened?.GetInvocationList().Length ?? 0) +
            (MessageReceived?.GetInvocationList().Length ?? 0) +
            (ErrorReceived?.GetInvocationList().Length ?? 0) + (Closed?.GetInvocationList().Length ?? 0);
        public Action<WebSocketCloseCode> CaptureClose() => Closed;
        public void Receive(string message) => MessageReceived?.Invoke(message);
        public void CloseFromPeer() { IsConnected = false; Closed?.Invoke(WebSocketCloseCode.Away); }
        public void Connect(string jwtToken = null) {
            JwtToken = jwtToken;
            if (FailOpen) { opened.TrySetException(new InvalidOperationException("Expected connection failure")); return; }
            if (DelayOpen) return;
            IsConnected = true;
            opened.TrySetResult();
            Opened?.Invoke();
        }
        public async UniTask ConnectAsync(string jwtToken = null, CancellationToken cancellationToken = default) {
            Connect(jwtToken);
            await opened.Task.AttachExternalCancellation(cancellationToken);
        }
        public UniTask SendStringAsync(string message) { LastJson = message; return UniTask.CompletedTask; }
        public UniTask SendJsonAsync<T>(T message) => SendStringAsync(JsonConvert.SerializeObject(message));
        public UniTask DisconnectAsync() {
            Disconnects++;
            IsConnected = false;
            Closed?.Invoke(WebSocketCloseCode.Normal);
            return UniTask.CompletedTask;
        }
        public void Abort() { Aborts++; IsConnected = false; }
    }
}
