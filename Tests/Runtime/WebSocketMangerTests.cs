using System;
using System.Collections;
using System.Linq;
using System.Threading;
using Cysharp.Threading.Tasks;
using NativeWebSocket;
using Network;
using NUnit.Framework;
using UnityEngine;
using UnityEngine.TestTools;

namespace Song.UnityNetworks.Tests {
    public class WebSocketMangerTests {
        private GameObject root;
        private TestWebSocketManger manager;
        [SetUp] public void SetUp() {
            root = new GameObject("Network module test");
            manager = root.AddComponent<TestWebSocketManger>();
        }
        [UnityTearDown] public IEnumerator TearDown() {
            if (root != null) UnityEngine.Object.Destroy(root);
            yield return null;
        }
        private FakeSocketClient Connect() {
            var client = new FakeSocketClient();
            manager.Clients.Enqueue(client);
            manager.ConnectSocketAsync("ws://example.test/socket").GetAwaiter().GetResult();
            return client;
        }
        [Test] public void ConnectUsesTokenAndForwardsRawMessagesToOverrideAndEvent() {
            manager.SetJwtToken("test-token");
            int opened = 0;
            string message = null;
            manager.SocketOpened += () => opened++;
            manager.MessageReceived += value => message = value;
            var client = Connect();
            client.Receive("{\"custom\":7}");
            Assert.That(client.JwtToken, Is.EqualTo("test-token"));
            Assert.That(manager.IsConnected, Is.True);
            Assert.That(opened, Is.EqualTo(1));
            Assert.That(message, Is.EqualTo("{\"custom\":7}"));
            Assert.That(manager.LastHandledMessage, Is.EqualTo(message));
        }
        [Test] public void CustomLogRegistrationDoesNotReplaceCoreRoutingAndCleanup() {
            manager.CustomRegistration = true;
            var client = Connect();
            client.Receive("project-specific payload");
            Assert.That(manager.Opens, Is.EqualTo(1));
            Assert.That(manager.LastHandledMessage, Is.EqualTo("project-specific payload"));
            manager.DisconnectSocketAsync().GetAwaiter().GetResult();
            Assert.That(manager.Unregistrations, Is.EqualTo(1));
            Assert.That(client.SubscriberCount, Is.Zero);
        }
        [Test] public void ExplicitDisconnectUnsubscribesAndDoesNotReportUnexpectedClose() {
            int unexpected = 0;
            manager.SocketDisconnected += () => unexpected++;
            var client = Connect();
            manager.DisconnectSocketAsync().GetAwaiter().GetResult();
            Assert.That(client.Disconnects, Is.EqualTo(1));
            Assert.That(client.SubscriberCount, Is.Zero);
            Assert.That(manager.IsConnected, Is.False);
            Assert.That(unexpected, Is.Zero);
        }
        [Test] public void PeerCloseClearsCurrentConnectionAndRaisesDisconnectOnce() {
            int disconnected = 0;
            manager.SocketDisconnected += () => disconnected++;
            var client = Connect();
            client.CloseFromPeer();
            client.CloseFromPeer();
            Assert.That(disconnected, Is.EqualTo(1));
            Assert.That(client.SubscriberCount, Is.Zero);
            Assert.That(manager.IsConnected, Is.False);
            Assert.Throws<InvalidOperationException>(() => manager.SendSocketJsonAsync(new { a = 1 }));
        }
        [Test] public void ReconnectIgnoresCapturedCallbacksFromPreviousClient() {
            var first = Connect();
            var staleClose = first.CaptureClose();
            var second = Connect();
            staleClose(WebSocketCloseCode.Abnormal);
            first.Receive("stale");
            manager.SendSocketJsonAsync(new { arbitrary = 123 }).GetAwaiter().GetResult();
            Assert.That(first.Disconnects, Is.EqualTo(1));
            Assert.That(first.SubscriberCount, Is.Zero);
            Assert.That(manager.IsConnected, Is.True);
            Assert.That(second.LastJson, Is.EqualTo("{\"arbitrary\":123}"));
            Assert.That(manager.LastHandledMessage, Is.Null);
        }
        [Test] public void CancellationAbortsPendingHandshakeAndUnsubscribes() {
            var client = new FakeSocketClient { DelayOpen = true };
            manager.Clients.Enqueue(client);
            using var cancellation = new CancellationTokenSource();
            var task = manager.ConnectSocketAsync("ws://example.test/socket", cancellation.Token);
            Assert.That(task.Status, Is.EqualTo(UniTaskStatus.Pending));
            cancellation.Cancel();
            Assert.Throws<OperationCanceledException>(() => task.GetAwaiter().GetResult());
            Assert.That(client.Aborts, Is.GreaterThan(0));
            Assert.That(client.SubscriberCount, Is.Zero);
        }
        [Test] public void DisconnectCancelsAHandshakeInsteadOfWaitingForOpen() {
            var client = new FakeSocketClient { DelayOpen = true };
            manager.Clients.Enqueue(client);
            var task = manager.ConnectSocketAsync("ws://example.test/socket");
            manager.DisconnectSocketAsync().GetAwaiter().GetResult();
            Assert.Throws<OperationCanceledException>(() => task.GetAwaiter().GetResult());
            Assert.That(client.SubscriberCount, Is.Zero);
        }
        [Test] public void FailedConnectReleasesClientAndAllowsAnotherConnection() {
            var client = new FakeSocketClient { FailOpen = true };
            manager.Clients.Enqueue(client);
            Assert.Throws<InvalidOperationException>(() => manager.ConnectSocketAsync("ws://example.test/socket").GetAwaiter().GetResult());
            Assert.That(client.SubscriberCount, Is.Zero);
            Assert.That(client.Aborts, Is.GreaterThan(0));
            Connect();
            Assert.That(manager.IsConnected, Is.True);
        }
        [UnityTest] public IEnumerator DestroyCancelsPendingConnectionAndDetachesAllHandlers() {
            var client = new FakeSocketClient { DelayOpen = true };
            manager.Clients.Enqueue(client);
            var task = manager.ConnectSocketAsync("ws://example.test/socket");
            UnityEngine.Object.Destroy(root);
            yield return null;
            Assert.Throws<OperationCanceledException>(() => task.GetAwaiter().GetResult());
            Assert.That(client.SubscriberCount, Is.Zero);
        }
        [Test] public void AlreadyCanceledConnectDoesNotReplaceCurrentConnection() {
            var client = Connect();
            var task = manager.ConnectSocketAsync("ws://example.test/other", new CancellationToken(true));
            Assert.Throws<OperationCanceledException>(() => task.GetAwaiter().GetResult());
            Assert.That(client.Disconnects, Is.Zero);
            Assert.That(manager.IsConnected, Is.True);
        }
        [Test] public void RuntimeAssemblyHasNoGameOrSingletonDependency() {
            var references = typeof(WebSocketManger).Assembly.GetReferencedAssemblies().Select(a => a.Name).ToArray();
            Assert.That(references, Does.Not.Contain("Assembly-CSharp"));
            Assert.That(typeof(WebSocketManger).BaseType, Is.EqualTo(typeof(MonoBehaviour)));
        }
    }
}
