using System;
using System.Threading;
using Cysharp.Threading.Tasks;
using NativeWebSocket;

namespace Network {
    public interface IWebSocketClient {
        event Action Opened;
        event Action<string> MessageReceived;
        event Action<string> ErrorReceived;
        event Action<WebSocketCloseCode> Closed;
        bool IsConnected { get; }
        void Connect(string jwtToken = null);
        UniTask ConnectAsync(string jwtToken = null, CancellationToken cancellationToken = default);
        UniTask SendStringAsync(string message);
        UniTask SendJsonAsync<T>(T message);
        UniTask DisconnectAsync();
        void Abort();
    }
}
