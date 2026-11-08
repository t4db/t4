package gateway_test

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/t4db/t4/internal/gateway"
)

// TestWatchOverWebsocket is the capability the gateway exists for.
//
// Watch is bidirectional, and no plain-HTTP framing carries that, which is why
// a browser cannot watch over grpc-web: grpc-web has no bidirectional
// streaming, so the client half-closes after its first message and the server
// tears the watch down before an event can arrive. Over a websocket the stream
// stays open in both directions.
func TestWatchOverWebsocket(t *testing.T) {
	base, conn := newGateway(t, gateway.Config{})
	wsURL := "ws" + strings.TrimPrefix(base, "http") + "/v3/watch"

	ws, resp, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		status := ""
		if resp != nil {
			status = resp.Status
		}
		t.Fatalf("websocket dial: %v (%s)", err, status)
	}
	defer func() { _ = ws.Close() }()

	if err := ws.WriteMessage(websocket.TextMessage,
		[]byte(`{"create_request":{"key":"`+b64("/watched")+`"}}`)); err != nil {
		t.Fatalf("send create: %v", err)
	}

	// The create notification confirms the server registered the watch.
	created := readWatchFrame(t, ws)
	if !created.Result.Created {
		t.Fatalf("first frame is not a create notification: %+v", created)
	}

	// Write over gRPC: the event must reach a websocket client.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := etcdserverpb.NewKVClient(conn).Put(ctx, &etcdserverpb.PutRequest{
		Key:   []byte("/watched"),
		Value: []byte("changed"),
	}); err != nil {
		t.Fatalf("grpc put: %v", err)
	}

	event := readWatchFrame(t, ws)
	if len(event.Result.Events) != 1 {
		t.Fatalf("want one event, got %+v", event.Result)
	}
	if got := event.Result.Events[0].Kv.Value; got != b64("changed") {
		t.Errorf("event value = %q, want %q", got, b64("changed"))
	}
}

// watchFrame is the websocket envelope grpc-gateway wraps a stream in.
type watchFrame struct {
	Result struct {
		Created bool `json:"created"`
		Events  []struct {
			Kv struct {
				Key   string `json:"key"`
				Value string `json:"value"`
			} `json:"kv"`
		} `json:"events"`
	} `json:"result"`
}

func readWatchFrame(tb testing.TB, ws *websocket.Conn) watchFrame {
	tb.Helper()
	if err := ws.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		tb.Fatalf("set deadline: %v", err)
	}
	_, msg, err := ws.ReadMessage()
	if err != nil {
		tb.Fatalf("read frame: %v", err)
	}
	var f watchFrame
	if err := json.Unmarshal(msg, &f); err != nil {
		tb.Fatalf("decode frame %s: %v", msg, err)
	}
	return f
}

// TestWatchRejectsGRPCWeb records the behaviour that sent someone looking for
// this feature in the first place: over grpc-web a watch "succeeds" and then
// delivers nothing. The gateway answers plainly instead.
func TestWatchRejectsGRPCWeb(t *testing.T) {
	base, _ := newGateway(t, gateway.Config{})

	req, _ := http.NewRequest(http.MethodPost, base+"/v3/watch", strings.NewReader(""))
	req.Header.Set("Content-Type", "application/grpc-web+proto")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	// The marshaler accepts any content type, so without an explicit guard
	// this is a 200 carrying an empty stream — a watch that looks established
	// and never delivers anything.
	if resp.StatusCode != http.StatusNotImplemented {
		t.Errorf("status = %d, want %d; a grpc-web watch must not look like it worked",
			resp.StatusCode, http.StatusNotImplemented)
	}
}
