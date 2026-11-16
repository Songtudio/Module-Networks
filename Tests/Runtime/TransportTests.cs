using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using Cysharp.Threading.Tasks;
using NativeWebSocket;
using Network;
using Newtonsoft.Json;
using NUnit.Framework;
using UnityEngine.TestTools;

namespace Song.UnityNetworks.Tests {
    public class TransportTests {
        private LoopbackServer server;
        [SetUp] public void SetUp() => server = new LoopbackServer();
        [TearDown] public void TearDown() => server.Dispose();
        public sealed class Payload { public int value; public string optional; }

        [UnityTest] public IEnumerator RestGetNormalizesSlashesAndCanClearBearerHeader() => UniTask.ToCoroutine(async () => {
            var client = new RestApiClient(server.HttpUrl + "/");
            client.SetJwtToken("test-token");
            client.SetHeader("X-Project", "generic");
            var response = await client.GetAsync<Payload>("/value?query=ok");
            Assert.That(response.value, Is.EqualTo(42));
            Assert.That(server.LastRequest.Path, Is.EqualTo("/value?query=ok"));
            Assert.That(server.LastRequest.Headers["Authorization"], Is.EqualTo("Bearer test-token"));
            Assert.That(server.LastRequest.Headers["X-Project"], Is.EqualTo("generic"));
            client.SetJwtToken(null);
            await client.GetAsync<Payload>("value");
            Assert.That(server.LastRequest.Headers.ContainsKey("Authorization"), Is.False);
        });

        [UnityTest] public IEnumerator RestWriteVerbsPreserveJsonAndUseCustomSerializerSettings() => UniTask.ToCoroutine(async () => {
            var client = new RestApiClient(server.HttpUrl, jsonSettings: new JsonSerializerSettings { NullValueHandling = NullValueHandling.Ignore });
            var payload = new Payload { value = 9 };
            foreach (var method in new[] { "POST", "PUT", "PATCH" }) {
                Payload response = method == "POST" ? await client.PostAsync<Payload, Payload>("echo", payload) :
                    method == "PUT" ? await client.PutAsync<Payload, Payload>("echo", payload) :
                    await client.PatchAsync<Payload, Payload>("echo", payload);
                Assert.That(response.value, Is.EqualTo(9));
                Assert.That(server.LastRequest.Method, Is.EqualTo(method));
                Assert.That(server.LastRequest.Body, Is.EqualTo("{\"value\":9}"));
                Assert.That(server.LastRequest.Headers["Content-Type"], Does.StartWith("application/json"));
            }
        });

        [UnityTest] public IEnumerator RestHandlesEmptyResponsesAndDelete() => UniTask.ToCoroutine(async () => {
            var client = new RestApiClient(server.HttpUrl);
            Assert.That(await client.GetAsync<Payload>("empty"), Is.Null);
            await client.DeleteAsync("empty");
            Assert.That(server.LastRequest.Method, Is.EqualTo("DELETE"));
        });

        [UnityTest] public IEnumerator RestFailureExposesStatusAndBodyWithoutEmbeddingUrlInMessage() => UniTask.ToCoroutine(async () => {
            var client = new RestApiClient(server.HttpUrl);
            RestApiException caught = null;
            try { await client.GetAsync<Payload>("error?token=not-for-logs"); }
            catch (RestApiException exception) { caught = exception; }
            Assert.That(caught, Is.Not.Null);
            Assert.That(caught.StatusCode, Is.EqualTo(422));
            Assert.That(caught.ResponseBody, Is.EqualTo("{\"error\":\"test_failure\"}"));
            Assert.That(caught.Message, Does.Not.Contain("not-for-logs"));
        });

        [UnityTest] public IEnumerator RestCancellationRemainsCancellation() => UniTask.ToCoroutine(async () => {
            var client = new RestApiClient(server.HttpUrl);
            using var cancellation = new CancellationTokenSource();
            var task = client.GetAsync<Payload>("slow", cancellation.Token);
            cancellation.Cancel();
            bool canceled = false;
            try { await task; } catch (OperationCanceledException) { canceled = true; }
            Assert.That(canceled, Is.True);
        });

        [UnityTest] public IEnumerator RestMalformedJsonIsNotSilentlyAccepted() => UniTask.ToCoroutine(async () => {
            var client = new RestApiClient(server.HttpUrl);
            bool invalid = false;
            try { await client.GetAsync<Payload>("invalid"); } catch (JsonException) { invalid = true; }
            Assert.That(invalid, Is.True);
        });

        [TestCase("ws://example.test")]
        [TestCase("not-a-url")]
        [TestCase("https://example.test/api?token=value")]
        public void RestRejectsInvalidBaseUrls(string url) => Assert.Throws<ArgumentException>(() => new RestApiClient(url));

        [UnityTest] public IEnumerator WebSocketConnectWaitsForOpenAndEchoesUtf8Json() => UniTask.ToCoroutine(async () => {
            var client = new WebSocketClient(server.SocketUrl + "/socket");
            var received = new UniTaskCompletionSource<string>();
            client.MessageReceived += value => received.TrySetResult(value);
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try {
                await client.ConnectAsync("test-token", deadline.Token);
                Assert.That(client.IsConnected, Is.True);
                Assert.That(server.LastRequest.Headers["Authorization"], Is.EqualTo("Bearer test-token"));
                await client.SendJsonAsync(new { text = "hello 한글", custom = 17 });
                string response = await received.Task.AttachExternalCancellation(deadline.Token);
                Assert.That(response, Is.EqualTo("{\"text\":\"hello 한글\",\"custom\":17}"));
            } finally { await client.DisconnectAsync(); }
            Assert.That(client.IsConnected, Is.False);
        });

        [UnityTest] public IEnumerator WebSocketQueuesMessagesDuringHandshakeInOrder() => UniTask.ToCoroutine(async () => {
            var client = new WebSocketClient(server.SocketUrl + "/delay");
            var messages = new List<string>();
            var complete = new UniTaskCompletionSource();
            client.MessageReceived += value => { messages.Add(value); if (messages.Count == 3) complete.TrySetResult(); };
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            try {
                client.Connect();
                await client.SendStringAsync("first");
                await client.SendStringAsync("second");
                // Hold Unity's event pump until the native handshake has opened:
                // a send in this window must still follow the pre-open queue.
                Assert.That(SpinWait.SpinUntil(() => client.IsConnected, 2000), Is.True);
                await client.SendStringAsync("third");
                await complete.Task.AttachExternalCancellation(deadline.Token);
                Assert.That(messages, Is.EqualTo(new[] { "first", "second", "third" }));
            } finally { await client.DisconnectAsync(); }
        });

        [UnityTest] public IEnumerator WebSocketCancelDuringHandshakeDoesNotLeaveConnectedSocket() => UniTask.ToCoroutine(async () => {
            var client = new WebSocketClient(server.SocketUrl + "/delay");
            using var cancellation = new CancellationTokenSource();
            var connect = client.ConnectAsync(cancellationToken: cancellation.Token);
            cancellation.Cancel();
            bool canceled = false;
            try { await connect; } catch (OperationCanceledException) { canceled = true; }
            Assert.That(canceled, Is.True);
            Assert.That(client.IsConnected, Is.False);
        });

        [UnityTest] public IEnumerator WebSocketCloseTimeoutIsBoundedAndReportsAbnormalClose() => UniTask.ToCoroutine(async () => {
            var client = new WebSocketClient(server.SocketUrl + "/no-close", TimeSpan.FromMilliseconds(80));
            WebSocketCloseCode? code = null;
            client.Closed += value => code = value;
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await client.ConnectAsync(cancellationToken: deadline.Token);
            var watch = Stopwatch.StartNew();
            await client.DisconnectAsync();
            Assert.That(watch.Elapsed.TotalSeconds, Is.LessThan(2));
            Assert.That(code, Is.EqualTo(WebSocketCloseCode.Abnormal));
            Assert.That(client.IsConnected, Is.False);
        });

        [UnityTest] public IEnumerator WebSocketPendingQueueIsBounded() => UniTask.ToCoroutine(async () => {
            var client = new WebSocketClient(server.SocketUrl + "/delay", maxPendingMessages: 1);
            try {
                client.Connect();
                await client.SendStringAsync("first");
                bool full = false;
                try { await client.SendStringAsync("second"); } catch (InvalidOperationException) { full = true; }
                Assert.That(full, Is.True);
            } finally { client.Abort(); }
        });

        [Test] public void WebSocketValidatesUrlAndSendState() {
            Assert.Throws<ArgumentException>(() => new WebSocketClient("https://example.test"));
            var client = new WebSocketClient("ws://example.test");
            Assert.Throws<InvalidOperationException>(() => client.SendStringAsync("hello").GetAwaiter().GetResult());
        }
    }
}
