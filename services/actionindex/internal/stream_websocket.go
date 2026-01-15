package internal

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/greymass/roborovski/libraries/chain"
	"github.com/greymass/roborovski/libraries/logger"
	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type wsSubscribeMessage struct {
	Type      string   `json:"type"`
	Contracts []string `json:"contracts,omitempty"`
	Receivers []string `json:"receivers,omitempty"`
	StartSeq  uint64   `json:"start_seq,omitempty"`
	Decode    *bool    `json:"decode,omitempty"`
}

type wsActionMessage struct {
	Type      string         `json:"type"`
	GlobalSeq uint64         `json:"global_seq"`
	BlockNum  uint32         `json:"block_num"`
	BlockTime uint32         `json:"block_time"`
	Contract  string         `json:"contract"`
	Action    string         `json:"action"`
	Receiver  string         `json:"receiver"`
	HexData   string         `json:"hex_data,omitempty"`
	Data      map[string]any `json:"data,omitempty"`
}

type wsHeartbeatMessage struct {
	Type    string `json:"type"`
	HeadSeq uint64 `json:"head_seq"`
	LibSeq  uint64 `json:"lib_seq"`
}

type wsCatchupCompleteMessage struct {
	Type    string `json:"type"`
	HeadSeq uint64 `json:"head_seq"`
	LibSeq  uint64 `json:"lib_seq"`
}

type wsErrorMessage struct {
	Type    string `json:"type"`
	Code    uint16 `json:"code"`
	Message string `json:"message"`
}

type StreamWebSocketServer struct {
	server   *StreamServer
	maxConns int

	httpServer *http.Server
	clients    map[uint64]*wsStreamClient
	clientsMu  sync.RWMutex
	nextID     atomic.Uint64

	closeChan chan struct{}
	closed    atomic.Bool
	wg        sync.WaitGroup
}

type wsStreamClient struct {
	id     uint64
	ws     *websocket.Conn
	client *StreamClient
	cancel context.CancelFunc
}

func NewStreamWebSocketServer(server *StreamServer, maxConns int) *StreamWebSocketServer {
	return &StreamWebSocketServer{
		server:    server,
		maxConns:  maxConns,
		clients:   make(map[uint64]*wsStreamClient),
		closeChan: make(chan struct{}),
	}
}

func (ws *StreamWebSocketServer) Listen(address string) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", ws.handleWebSocket)
	mux.HandleFunc("/stream", ws.handleWebSocket)

	ws.httpServer = &http.Server{
		Addr:         address,
		Handler:      mux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
	}

	go func() {
		if err := ws.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Warning("WebSocket server error: %v", err)
		}
	}()

	return nil
}

func (ws *StreamWebSocketServer) handleWebSocket(w http.ResponseWriter, r *http.Request) {
	if ws.closed.Load() {
		http.Error(w, "server shutting down", http.StatusServiceUnavailable)
		return
	}

	ws.clientsMu.RLock()
	connCount := len(ws.clients)
	ws.clientsMu.RUnlock()

	if connCount >= ws.maxConns {
		http.Error(w, "max connections reached", http.StatusServiceUnavailable)
		return
	}

	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		OriginPatterns:  []string{"*"},
		CompressionMode: websocket.CompressionDisabled,
	})
	if err != nil {
		logger.Warning("WebSocket accept error: %v", err)
		return
	}

	ctx, cancel := context.WithCancel(context.Background())

	var subMsg wsSubscribeMessage
	if err := wsjson.Read(ctx, conn, &subMsg); err != nil {
		cancel()
		conn.Close(websocket.StatusProtocolError, "invalid subscribe")
		return
	}

	if subMsg.Type != "subscribe" {
		cancel()
		ws.sendError(ctx, conn, ActionErrorInvalidRequest, "expected subscribe message")
		conn.Close(websocket.StatusProtocolError, "invalid message type")
		return
	}

	if len(subMsg.Contracts) == 0 && len(subMsg.Receivers) == 0 {
		cancel()
		ws.sendError(ctx, conn, ActionErrorInvalidRequest, "must specify contracts or receivers")
		conn.Close(websocket.StatusProtocolError, "no filter")
		return
	}

	filter := ActionFilter{
		Contracts: make(map[uint64]struct{}),
		Receivers: make(map[uint64]struct{}),
	}
	for _, c := range subMsg.Contracts {
		filter.Contracts[chain.StringToName(c)] = struct{}{}
	}
	for _, r := range subMsg.Receivers {
		filter.Receivers[chain.StringToName(r)] = struct{}{}
	}

	decode := true
	if subMsg.Decode != nil {
		decode = *subMsg.Decode
	}

	clientID := ws.nextID.Add(1)
	client := NewStreamClient(clientID, ws.server, filter, subMsg.StartSeq, decode)

	wsc := &wsStreamClient{
		id:     clientID,
		ws:     conn,
		client: client,
		cancel: cancel,
	}

	ws.clientsMu.Lock()
	ws.clients[clientID] = wsc
	connCount = len(ws.clients)
	ws.clientsMu.Unlock()

	logger.Printf("stream", "WebSocket client %d connected (%d/%d)", clientID, connCount, ws.maxConns)

	ws.wg.Add(1)
	go func() {
		defer ws.wg.Done()
		ws.recvLoop(ctx, wsc)
	}()

	err = client.Run(ctx,
		func(action StreamedAction) error {
			return ws.sendAction(ctx, conn, action, decode)
		},
		func() error {
			head, lib := ws.server.broadcaster.GetState()
			return ws.sendCatchupComplete(ctx, conn, head, lib)
		},
	)

	ws.removeClient(wsc)

	if err != nil && err != context.Canceled {
		logger.Printf("stream", "WebSocket client %d error: %v", clientID, err)
	}
}

func (ws *StreamWebSocketServer) recvLoop(ctx context.Context, wsc *wsStreamClient) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var msg json.RawMessage
		if err := wsjson.Read(ctx, wsc.ws, &msg); err != nil {
			wsc.cancel()
			return
		}

		var baseMsg struct {
			Type string `json:"type"`
		}
		if err := json.Unmarshal(msg, &baseMsg); err != nil {
			continue
		}

		switch baseMsg.Type {
		case "ack":
			var ack struct {
				Seq uint64 `json:"seq"`
			}
			if err := json.Unmarshal(msg, &ack); err == nil && wsc.client.sub != nil {
				wsc.client.sub.Ack(ack.Seq)
			}
		}
	}
}

func (ws *StreamWebSocketServer) removeClient(wsc *wsStreamClient) {
	ws.clientsMu.Lock()
	delete(ws.clients, wsc.id)
	connCount := len(ws.clients)
	ws.clientsMu.Unlock()

	wsc.cancel()
	wsc.client.Close()
	wsc.ws.Close(websocket.StatusNormalClosure, "done")

	actionsSent, uptime := wsc.client.Stats()
	logger.Printf("stream", "WebSocket client %d disconnected (sent %d actions in %v, %d/%d)",
		wsc.id, actionsSent, uptime.Round(time.Second), connCount, ws.maxConns)
}

func (ws *StreamWebSocketServer) sendAction(ctx context.Context, conn *websocket.Conn, action StreamedAction, decode bool) error {
	msg := wsActionMessage{
		Type:      "action",
		GlobalSeq: action.GlobalSeq,
		BlockNum:  action.BlockNum,
		BlockTime: action.BlockTime,
		Contract:  chain.NameToString(action.Contract),
		Action:    chain.NameToString(action.Action),
		Receiver:  chain.NameToString(action.Receiver),
	}

	if decode && len(action.ActionData) > 0 {
		if ws.server.abiReader != nil {
			decoded, err := ws.server.abiReader.Decode(action.Contract, action.Action, action.ActionData, action.BlockNum)
			if err == nil && decoded != nil {
				msg.Data = decoded
			}
		}
		if msg.Data == nil {
			msg.HexData = hex.EncodeToString(action.ActionData)
		}
	} else if len(action.ActionData) > 0 {
		msg.HexData = hex.EncodeToString(action.ActionData)
	}

	return wsjson.Write(ctx, conn, msg)
}

func (ws *StreamWebSocketServer) sendCatchupComplete(ctx context.Context, conn *websocket.Conn, headSeq, libSeq uint64) error {
	msg := wsCatchupCompleteMessage{
		Type:    "catchup_complete",
		HeadSeq: headSeq,
		LibSeq:  libSeq,
	}
	return wsjson.Write(ctx, conn, msg)
}

func (ws *StreamWebSocketServer) sendError(ctx context.Context, conn *websocket.Conn, code uint16, message string) {
	msg := wsErrorMessage{
		Type:    "error",
		Code:    code,
		Message: message,
	}
	wsjson.Write(ctx, conn, msg)
}

func (ws *StreamWebSocketServer) Close() error {
	if !ws.closed.CompareAndSwap(false, true) {
		return nil
	}

	close(ws.closeChan)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if ws.httpServer != nil {
		ws.httpServer.Shutdown(ctx)
	}

	ws.clientsMu.Lock()
	for _, wsc := range ws.clients {
		wsc.cancel()
		wsc.client.Close()
		wsc.ws.Close(websocket.StatusGoingAway, "server shutdown")
	}
	ws.clientsMu.Unlock()

	ws.wg.Wait()
	return nil
}
