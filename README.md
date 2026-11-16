# Song Unity Networks

프로젝트의 통신 기반 코드를 분리한 범용 UPM 패키지다. 원본 CrawlStars에는 적용하지 않았다.

## 구성

| 타입 | 역할 |
|---|---|
| `RestApiClient` | GET·POST·PUT·PATCH·DELETE, JSON 직렬화, 취소, HTTP 타임아웃과 헤더 |
| `RestApiException` | HTTP 상태 코드, 응답 본문, 전송 오류 제공 |
| `WebSocketClient` | NativeWebSocket 기반 연결·송수신·닫기, 연결 중 메시지 큐 |
| `IWebSocketClient` | 프로젝트별 클라이언트 교체와 테스트용 인터페이스 |
| `WebSocketManger` | 연결 교체, 취소, 이벤트 구독·해제와 확장 가능한 메시지·로그 처리 |

`WebSocketManger`는 요청한 철자의 클래스명이다. `Network` 네임스페이스를 유지한다. 싱글턴을 상속하지 않는 MonoBehaviour이므로 씬에 두거나 프로젝트의 매니저가 상속해서 사용한다. 자동 연결이나 설정 파일 로딩은 하지 않는다.

매칭, room/player/session DTO, Ready ACK, 스냅샷, 게임 종료, 입력 tick, `NetworkConfig`, 캐릭터·모드·플레이어 매니저는 포함하지 않는다. 수신 JSON의 필드명·대소문자·프로토콜은 프로젝트가 결정한다.

## 설치

Unity 6000.3 이상을 대상으로 한다. 검증 기준은 6000.3.15f1이다.

1. Package Manager의 **Install package from git URL**에서 다음 두 의존성을 먼저 설치한다.

```text
https://github.com/Cysharp/UniTask.git?path=src/UniTask/Assets/Plugins/UniTask#2.5.10
https://github.com/endel/NativeWebSocket.git#522f0c3f5c2ef7273737707effaa840c65e20e98
```

두 번째 URL은 CrawlStars에서 실제 사용 중인 NativeWebSocket 2.0.4 커밋이다. UPM은 패키지 간 Git 의존성을 지원하지 않으므로 이 두 Git 패키지는 호스트 프로젝트가 설치해야 한다. Newtonsoft Json 3.2.2와 UnityWebRequest 모듈은 `package.json`의 일반 의존성으로 선언한다.

2. **Install package from disk**에서 이 폴더의 `package.json`을 선택한다. 또는 폴더 전체를 대상 프로젝트의 `Packages/com.song.unity-networks`에 넣는다.
3. Git으로 배포할 때는 이 폴더의 내용이 저장소 루트에 오도록 복사하고 버전 태그를 만든다. 별도 Tools·Documentation 폴더는 필요 없다.

## REST 사용

```csharp
using System.Threading;
using Network;

var api = new RestApiClient("https://api.example.com", timeoutSeconds: 30);
api.SetJwtToken(accessToken); // null 또는 빈 문자열을 전달하면 Authorization 제거
api.SetHeader("X-Project", "my-project");
var response = await api.GetAsync<MyResponse>("items/123", cancellationToken);
```

`MyResponse`는 프로젝트의 DTO다. 생성자의 `JsonSerializerSettings`로 직렬화 규칙을 바꿀 수 있다. 경로는 base URL에 붙일 상대 경로를 전달한다. 선행·후행 슬래시는 정리하고 JSON 속성명과 envelope는 자동 변환하지 않는다.

HTTP·전송 실패는 `RestApiException`으로 확인한다. `StatusCode`, `ResponseBody`, `TransportError`를 제공하며, 취소는 `OperationCanceledException`을 유지한다. 빈 응답은 `default(TResponse)`를 반환한다. 타임아웃 기본값은 30초이며, 생성자에서 0을 지정하면 UnityWebRequest의 무제한 설정을 사용한다.

## WebSocket 사용과 메시지 커스텀

```csharp
using Network;

public class ProjectSocket : WebSocketManger {
    protected override void HandleSocketMessage(string message) {
        // 프로젝트의 JSON/DTO/메시지 라우터로 처리한다.
        base.HandleSocketMessage(message); // MessageReceived 이벤트도 전달하려면 호출
    }

    protected override void LogSocketOpened() {
        // 프로젝트 로거로 연결 성공 기록
    }

    protected override void LogSocketError(string error) {
        // 프로젝트 로거로 오류 기록
    }
}
```

호출 측의 async 메서드에서 씬의 `ProjectSocket` 컴포넌트를 사용한다.

```csharp
socket.SetJwtToken(accessToken);
await socket.ConnectSocketAsync("wss://example.com/socket", cancellationToken);
await socket.SendSocketJsonAsync(new { action = "subscribe", channel = "updates" });
await socket.DisconnectSocketAsync();
```

`ConnectSocketAsync`는 경로 조각이 아닌 **완전한 ws/wss URL**을 받고 실제 Open 이벤트까지 기다린다. `WebSocketClient.Connect()`는 기존처럼 연결 시작 후 바로 반환하고, `ConnectAsync()`는 Open을 기다린다. 네이티브 라이브러리의 `Connect()` 작업이 연결 종료까지 유지되는 점과 구분한다.

공개 이벤트는 `SocketOpened`, `MessageReceived`, `SocketErrorReceived`, `SocketDisconnected`다. `SocketDisconnected`는 서버 측 종료 등 현재 연결의 예기치 않은 닫힘에만 발생한다. 명시적 종료와 연결 교체에는 발생하지 않는다. 명시적 종료는 반환 작업을 await해서 완료를 확인한다.

## 이벤트 구독 자체를 커스텀

```csharp
protected override void RegisterSocketLogEvents(IWebSocketClient client) {
    base.RegisterSocketLogEvents(client);
    client.Opened += ReportConnected;
}

protected override void UnregisterSocketLogEvents(IWebSocketClient client) {
    client.Opened -= ReportConnected;
    base.UnregisterSocketLogEvents(client);
}

private void ReportConnected() { /* 프로젝트별 처리 */ }
```

등록·해제는 한 쌍으로 재정의한다. 로그 등록과 내부 메시지 라우팅·닫힘 감지는 별개이므로 로그 메서드를 바꿔도 연결 수명주기 처리는 유지된다. 클라이언트 구현까지 바꾸려면 `CreateSocketClient(string url)`을 재정의해서 `IWebSocketClient` 구현을 반환한다.

## 연결 수명주기

- 새 연결은 이전 연결을 정리한다. 진행 중인 핸드셰이크도 취소할 수 있다.
- 호출자 토큰 취소, `DisconnectSocketAsync`, 객체 파괴 시 진행 중인 연결 대기를 정리한다.
- `OnDestroy`와 `OnApplicationQuit`을 재정의하면 `base`를 호출한다. 즉시 종료는 `AbortSocket()`을 사용한다.
- 연결이 교체되거나 닫히면 기본 이벤트와 로그 구독을 해제한다. 이전 클라이언트의 지연 이벤트가 새 연결을 정리하지 않도록 연결 객체를 구분한다.
- `WebSocketClient`는 연결 세션 하나당 새 인스턴스를 사용한다. `WebSocketManger`는 재사용할 수 있다.
- 연결 중 큐의 기본 상한은 256개이고, 닫기 제한 시간은 3초다. `WebSocketClient` 생성자로 바꿀 수 있다.
- 연결이 없거나 큐가 가득 찬 경우 전송은 예외를 반환한다. 전송 완료는 상대 서버의 처리·ACK 완료를 뜻하지 않는다.
- Unity 메인 스레드에서 생성·호출한다. NativeWebSocket 2.x는 Unity SynchronizationContext로 이벤트를 전달하므로 별도 Update dispatch를 넣지 않는다.
- WebGL에서는 브라우저 제약으로 Authorization 헤더가 적용되지 않는다. URL·쿠키·서브프로토콜 등 인증 방법은 프로젝트와 서버 계약에 맞춰 외부에서 결정한다. 패키지는 token query를 자동 추가하지 않는다.
- 자동 재연결, 서버별 heartbeat, 바이너리 프로토콜은 포함하지 않는다.

## CrawlStars에서 나중에 적용

현재 프로젝트의 원본 파일은 유지했다. 실제 설치 시에는 아래 두 원본과 각각의 `.meta`를 패키지 구현으로 교체해 클래스·GUID 중복을 피한다.

```text
Assets/Scripts/Network/RestApiClient.cs
Assets/Scripts/Network/WebSocketClient.cs
```

기존 `NetworkManager`, `NetworkConfig`, DTO는 프로젝트에 남긴다. 기존 `NetworkManager`는 동일한 `Network.RestApiClient`·`Network.WebSocketClient` API를 계속 참조할 수 있다. `WebSocketManger`를 쓰도록 상속 또는 구성 방식으로 변경하는 작업은 아직 적용하지 않았다. 별도 프로젝트 asmdef가 있으면 `Song.UnityNetworks` 참조를 추가한다.

기존 클라이언트의 생성자·HTTP 동사·JWT 설정·WebSocket 이벤트·Connect·전송·종료 메서드를 유지한다. HTTP 오류를 일관된 예외로 전달하고, 종료된 연결로의 전송은 조용히 버리지 않고 예외로 보고하는 차이가 있다.

## 테스트

Unity Test Framework가 있는 호스트에서 테스트 대상 패키지(`testables`)에 `com.song.unity-networks`를 추가하고 PlayMode 테스트를 실행한다.

```powershell
unity test <검증프로젝트경로> --mode PlayMode --filter Song.UnityNetworks --output networks-tests.xml
```

테스트에는 가짜 클라이언트를 이용한 매니저 확장·구독 해제·재연결·취소·파괴와, 외부 서버 없이 실제 TCP loopback으로 수행하는 HTTP 동사·JSON·헤더·오류·취소·WebSocket UTF-8 echo·전송 순서·닫기 타임아웃 검사가 포함된다. 테스트는 Unity Editor에서 실행한다.

2026-11-16, Unity 6000.3.15f1·UniTask 2.5.10·NativeWebSocket 2.0.4·Newtonsoft Json 3.2.2·Test Framework 1.6.0 환경의 복제 CrawlStars 프로젝트에서 **26개 통과, 실패 0개**를 확인했다. 기존 게임 스크립트도 공용 클라이언트를 패키지 타입으로 교체한 상태에서 함께 컴파일했다. 원본 프로젝트에는 패키지를 적용하지 않았다.

실제 서비스 서버, WebGL·모바일·IL2CPP Player 빌드와 프로젝트 전체 게임 흐름은 별도 검증이 필요하다.
