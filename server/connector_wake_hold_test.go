package server

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/itzg/mc-router/mcproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// heldClient is a fake Java client in the login state that answers login plugin requests the
// way vanilla does ("not understood") and reports the first other packet it receives.
type heldClient struct {
	conn       net.Conn
	keepAlives atomic.Int32
	// other receives the payload (packet ID first) of the first packet that is not a plugin
	// request, or nil when the connection ends without one.
	other chan []byte
}

func loginHeld(t *testing.T, ln net.Listener, serverAddress string, protocol mcproto.ProtocolVersion, answer bool) (*heldClient, []byte) {
	t.Helper()
	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	var sent bytes.Buffer
	require.NoError(t, writeTestPacket(&sent, mcproto.PacketIdHandshake, func(w io.Writer) {
		_ = mcproto.WriteVarInt(w, int32(protocol))
		_ = mcproto.WriteString(w, serverAddress)
		_, _ = w.Write([]byte{0x63, 0xdd})
		_ = mcproto.WriteVarInt(w, int32(mcproto.StateLogin))
	}))
	require.NoError(t, writeTestPacket(&sent, mcproto.PacketIdLogin, func(w io.Writer) {
		_ = mcproto.WriteString(w, "player1")
	}))
	_, err = conn.Write(sent.Bytes())
	require.NoError(t, err)

	c := &heldClient{conn: conn, other: make(chan []byte, 1)}
	go func() {
		reader := bufio.NewReader(conn)
		for {
			frame, err := mcproto.ReadFrame(reader, conn.RemoteAddr())
			if err != nil {
				c.other <- nil
				return
			}
			payload := bytes.NewReader(frame.Payload)
			packetID, _ := mcproto.ReadVarInt(payload)
			if packetID != mcproto.PacketIdLoginPluginRequest {
				c.other <- frame.Payload
				return
			}
			messageID, _ := mcproto.ReadVarInt(payload)
			channel, _ := mcproto.ReadString(payload)
			if channel != wakeHoldChannel {
				c.other <- frame.Payload
				return
			}
			c.keepAlives.Add(1)
			if answer {
				_ = writeTestPacket(conn, mcproto.PacketIdLoginPluginResponse, func(w io.Writer) {
					_ = mcproto.WriteVarInt(w, int32(messageID))
					_, _ = w.Write([]byte{0x00}) // successful = false
				})
			}
		}
	}()
	return c, sent.Bytes()
}

func (c *heldClient) next(t *testing.T, timeout time.Duration) []byte {
	t.Helper()
	select {
	case payload := <-c.other:
		return payload
	case <-time.After(timeout):
		t.Fatal("no packet from the router")
		return nil
	}
}

func disconnectReason(t *testing.T, payload []byte) string {
	t.Helper()
	require.NotNil(t, payload, "connection closed without a disconnect")
	r := bytes.NewReader(payload)
	packetID, err := mcproto.ReadVarInt(r)
	require.NoError(t, err)
	require.Equal(t, mcproto.PacketIdLoginDisconnect, packetID)
	reason, err := mcproto.ReadString(r)
	require.NoError(t, err)
	var component map[string]string
	require.NoError(t, json.Unmarshal([]byte(reason), &component))
	return component["text"]
}

// newHoldConnector returns a connector with short hold intervals, listening on loopback.
func newHoldConnector(t *testing.T, routes IRoutes, downScaler IDownScaler, holdTimeout time.Duration) net.Listener {
	t.Helper()
	c := NewConnector(t.Context(), routes, downScaler, discardMetricsBuilder{}.BuildConnectorMetrics(), false, false, nil, false)
	c.UseWakeKickMessage(wakeKickMessage)
	c.UseWakeHoldTimeout(holdTimeout)
	c.wakeHoldKeepAliveInterval = 50 * time.Millisecond
	c.wakeHoldPollInterval = 10 * time.Millisecond

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go c.acceptConnections(ln, 100, 0)
	return ln
}

// startBackend accepts connections like a server (readiness probes included), and for the first
// one that sends data reports what it received and answers with a marker packet.
func startBackend(t *testing.T) (string, <-chan []byte) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	received := make(chan []byte, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(conn net.Conn) {
				defer conn.Close()
				_ = conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
				data, _ := io.ReadAll(conn) // until EOF (probe) or the deadline (the client waits)
				if len(data) == 0 {
					return
				}
				received <- data
				_ = mcproto.WriteLoginDisconnect(conn, "from backend")
				time.Sleep(time.Second) // stay connected like a player in the world
			}(conn)
		}
	}()
	return ln.Addr().String(), received
}

func TestConnectorWakeHoldConnectsWhenBackendWakes(t *testing.T) {
	const scaleDownDelay = 300 * time.Millisecond
	backend, received := startBackend(t)

	downScaler := NewDownScaler(true, scaleDownDelay)
	routes := NewRoutes(t.Context()).WithDownScaler(downScaler)
	releaseWaker := make(chan struct{})
	var wakerCalls atomic.Int32
	waker := func(ctx context.Context) (string, error) {
		wakerCalls.Add(1)
		select {
		case <-releaseWaker:
		case <-ctx.Done():
		}
		return backend, nil
	}
	var scaleDowns atomic.Int32
	sleeper := func(context.Context) error {
		scaleDowns.Add(1)
		return nil
	}
	// backend "" = scaled down; the address comes from the waker
	routes.CreateMapping("mc.example.com", "", TestingScalingTarget("hold-backend"), RouteOptions{Waker: waker, Sleeper: sleeper, AsleepMOTD: "asleep", LoadingMOTD: "loading"})
	ln := newHoldConnector(t, routes, downScaler, 10*time.Second)

	client, sent := loginHeld(t, ln, "mc.example.com", mcproto.ProtocolVersion1_21_5, true)
	// Held past several keep-alive intervals without a kick
	require.Eventually(t, func() bool { return client.keepAlives.Load() >= 3 }, 2*time.Second, 10*time.Millisecond)
	assert.Equal(t, int32(1), wakerCalls.Load())
	close(releaseWaker)

	// The backend gets exactly the client's handshake and login, none of the keep-alive answers
	select {
	case data := <-received:
		assert.Equal(t, sent, data)
	case <-time.After(2 * time.Second):
		t.Fatal("held login was never relayed to the woken backend")
	}
	assert.Equal(t, "from backend", disconnectReason(t, client.next(t, 2*time.Second)))

	// The relayed player cancelled the scale-down armed by the background wake
	time.Sleep(scaleDownDelay * 2)
	assert.Equal(t, int32(0), scaleDowns.Load(), "server scaled down while the held player was connected")
}

func TestConnectorWakeHoldKicksAfterTimeout(t *testing.T) {
	routes := NewRoutes(t.Context())
	waker := func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}
	routes.CreateMapping("mc.example.com", "", TestingScalingTarget("slow-backend"), RouteOptions{Waker: waker, AsleepMOTD: "asleep", LoadingMOTD: "loading"})
	ln := newHoldConnector(t, routes, nil, 300*time.Millisecond)

	client, _ := loginHeld(t, ln, "mc.example.com", mcproto.ProtocolVersion1_21_5, true)
	assert.Equal(t, wakeKickMessage, disconnectReason(t, client.next(t, 2*time.Second)))
	assert.GreaterOrEqual(t, client.keepAlives.Load(), int32(2))
}

// Clients before 1.13 have no login plugin request: they are held without keep-alives and
// still get the kick message at the end.
func TestConnectorWakeHoldLegacyClientGetsNoKeepAlives(t *testing.T) {
	routes := NewRoutes(t.Context())
	waker := func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}
	routes.CreateMapping("mc.example.com", "", TestingScalingTarget("legacy-backend"), RouteOptions{Waker: waker, AsleepMOTD: "asleep", LoadingMOTD: "loading"})
	ln := newHoldConnector(t, routes, nil, 300*time.Millisecond)

	client, _ := loginHeld(t, ln, "mc.example.com", 340 /* 1.12.2 */, true)
	assert.Equal(t, wakeKickMessage, disconnectReason(t, client.next(t, 2*time.Second)))
	assert.Equal(t, int32(0), client.keepAlives.Load())
}

// A client that stops answering is gone: the router drops it without kicking or relaying.
func TestConnectorWakeHoldDropsSilentClient(t *testing.T) {
	routes := NewRoutes(t.Context())
	waker := func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}
	routes.CreateMapping("mc.example.com", "", TestingScalingTarget("silent-backend"), RouteOptions{Waker: waker, AsleepMOTD: "asleep", LoadingMOTD: "loading"})
	ln := newHoldConnector(t, routes, nil, 10*time.Second)

	client, _ := loginHeld(t, ln, "mc.example.com", mcproto.ProtocolVersion1_21_5, false)
	assert.Nil(t, client.next(t, 2*time.Second), "expected the connection to be closed")
	assert.Equal(t, int32(1), client.keepAlives.Load())
}

// Without a hold timeout the existing immediate kick is unchanged.
func TestConnectorWakeHoldDisabledKicksImmediately(t *testing.T) {
	routes := NewRoutes(t.Context())
	waker := func(ctx context.Context) (string, error) {
		<-ctx.Done()
		return "", ctx.Err()
	}
	routes.CreateMapping("mc.example.com", "", TestingScalingTarget("kick-backend"), RouteOptions{Waker: waker, AsleepMOTD: "asleep", LoadingMOTD: "loading"})
	ln := newHoldConnector(t, routes, nil, 0)

	client, _ := loginHeld(t, ln, "mc.example.com", mcproto.ProtocolVersion1_21_5, true)
	assert.Equal(t, wakeKickMessage, disconnectReason(t, client.next(t, time.Second)))
	assert.Equal(t, int32(0), client.keepAlives.Load())
}
