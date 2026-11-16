using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Song.UnityNetworks.Tests {
    // Small real HTTP/WebSocket loopback peer. No external services or Python tools.
    internal sealed class LoopbackServer : IDisposable {
        internal sealed class Request {
            public string Method, Path, Body;
            public readonly Dictionary<string, string> Headers = new(StringComparer.OrdinalIgnoreCase);
        }
        private readonly TcpListener listener = new(IPAddress.Loopback, 0);
        private readonly CancellationTokenSource lifetime = new();
        private readonly ConcurrentBag<TcpClient> clients = new();
        public readonly ConcurrentQueue<string> Received = new();
        public volatile Request LastRequest;
        public string HttpUrl { get; }
        public string SocketUrl => HttpUrl.Replace("http://", "ws://");
        public LoopbackServer() {
            listener.Start();
            HttpUrl = "http://127.0.0.1:" + ((IPEndPoint)listener.LocalEndpoint).Port;
            _ = AcceptAsync();
        }
        private async Task AcceptAsync() {
            while (!lifetime.IsCancellationRequested) {
                try {
                    var client = await listener.AcceptTcpClientAsync().ConfigureAwait(false);
                    clients.Add(client);
                    _ = ServeAsync(client);
                } catch (ObjectDisposedException) { break; }
                catch (SocketException) { break; }
            }
        }
        private async Task ServeAsync(TcpClient client) {
            using (client) {
                try {
                    var stream = client.GetStream();
                    var header = new List<byte>();
                    var one = new byte[1];
                    while (header.Count < 16384) {
                        if (await stream.ReadAsync(one, 0, 1, lifetime.Token).ConfigureAwait(false) == 0) return;
                        header.Add(one[0]);
                        int n = header.Count;
                        if (n >= 4 && header[n - 4] == 13 && header[n - 3] == 10 && header[n - 2] == 13 && header[n - 1] == 10) break;
                    }
                    var lines = Encoding.ASCII.GetString(header.ToArray()).Split(new[] { "\r\n" }, StringSplitOptions.None);
                    var first = lines[0].Split(' ');
                    var request = new Request { Method = first[0], Path = first[1] };
                    foreach (var line in lines) {
                        int colon = line.IndexOf(':');
                        if (colon > 0) request.Headers[line.Substring(0, colon)] = line.Substring(colon + 1).Trim();
                    }
                    if (request.Headers.TryGetValue("Content-Length", out var length)) {
                        request.Body = Encoding.UTF8.GetString(await ReadAsync(stream, int.Parse(length)).ConfigureAwait(false));
                    }
                    LastRequest = request;
                    if (request.Headers.ContainsKey("Sec-WebSocket-Key")) {
                        if (request.Path.StartsWith("/delay")) await Task.Delay(500, lifetime.Token).ConfigureAwait(false);
                        using var sha = SHA1.Create();
                        string accept = Convert.ToBase64String(sha.ComputeHash(Encoding.ASCII.GetBytes(
                            request.Headers["Sec-WebSocket-Key"] + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")));
                        await WriteAsync(stream, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n\r\n").ConfigureAwait(false);
                        while (!lifetime.IsCancellationRequested) {
                            var prefix = await ReadAsync(stream, 2).ConfigureAwait(false);
                            int opcode = prefix[0] & 15;
                            int size = prefix[1] & 127;
                            if (size == 126) { var ext = await ReadAsync(stream, 2).ConfigureAwait(false); size = (ext[0] << 8) | ext[1]; }
                            if (size == 127) throw new IOException("Oversized test frame");
                            var mask = (prefix[1] & 128) != 0 ? await ReadAsync(stream, 4).ConfigureAwait(false) : null;
                            var payload = await ReadAsync(stream, size).ConfigureAwait(false);
                            if (mask != null) for (int i = 0; i < size; i++) payload[i] ^= mask[i % 4];
                            if (opcode == 8 && request.Path.StartsWith("/no-close")) {
                                await Task.Delay(10000, lifetime.Token).ConfigureAwait(false);
                                return;
                            }
                            if (opcode == 1) Received.Enqueue(Encoding.UTF8.GetString(payload));
                            byte[] frame = new byte[2 + (size >= 126 ? 2 : 0) + size];
                            frame[0] = (byte)(128 | (opcode == 9 ? 10 : opcode));
                            int offset = 2;
                            if (size >= 126) { frame[1] = 126; frame[2] = (byte)(size >> 8); frame[3] = (byte)size; offset = 4; }
                            else frame[1] = (byte)size;
                            Buffer.BlockCopy(payload, 0, frame, offset, size);
                            await stream.WriteAsync(frame, 0, frame.Length, lifetime.Token).ConfigureAwait(false);
                            if (opcode == 8) return;
                        }
                        return;
                    }
                    if (request.Path.StartsWith("/slow")) await Task.Delay(10000, lifetime.Token).ConfigureAwait(false);
                    string status = request.Path.StartsWith("/error") ? "422 Unprocessable Entity" :
                        request.Path.StartsWith("/empty") ? "204 No Content" : "200 OK";
                    string body = status.StartsWith("422") ? "{\"error\":\"test_failure\"}" :
                        status.StartsWith("204") ? "" : request.Path.StartsWith("/invalid") ? "not-json" :
                        string.IsNullOrEmpty(request.Body) ? "{\"value\":42}" : request.Body;
                    int bytes = Encoding.UTF8.GetByteCount(body);
                    await WriteAsync(stream, "HTTP/1.1 " + status + "\r\nContent-Type: application/json\r\nContent-Length: " + bytes + "\r\nConnection: close\r\n\r\n" + body).ConfigureAwait(false);
                } catch (IOException) { }
                catch (OperationCanceledException) { }
                catch (ObjectDisposedException) { }
                catch (SocketException) { }
            }
        }
        private Task WriteAsync(NetworkStream stream, string text) {
            byte[] bytes = Encoding.UTF8.GetBytes(text);
            return stream.WriteAsync(bytes, 0, bytes.Length, lifetime.Token);
        }
        private async Task<byte[]> ReadAsync(NetworkStream stream, int count) {
            var bytes = new byte[count];
            for (int offset = 0; offset < count;) {
                int read = await stream.ReadAsync(bytes, offset, count - offset, lifetime.Token).ConfigureAwait(false);
                if (read == 0) throw new IOException("Peer closed");
                offset += read;
            }
            return bytes;
        }
        public void Dispose() {
            lifetime.Cancel();
            listener.Stop();
            foreach (var client in clients) client.Dispose();
            // In-flight server tasks may still read Token while cancellation unwinds.
        }
    }
}
