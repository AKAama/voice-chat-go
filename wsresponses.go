package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// wsresponses 代理浏览器到网关 /v1/responses 的 Responses WebSocket。
// 与 Realtime 不同：这是"文本聊天"通道，一轮一个 response.create，
// 连接复用且模型在首次使用后被网关锁定（换模型需重连）。
func handleWSResponses(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		cfg, err := store.GetConfig()
		if err != nil {
			http.Error(w, "读取配置失败: "+err.Error(), http.StatusInternalServerError)
			return
		}
		if cfg.Base == "" || cfg.Key == "" {
			http.Error(w, "请先在设置中配置平台地址和 API Key", http.StatusPreconditionRequired)
			return
		}
		if cfg.TextModel == "" {
			http.Error(w, "请先在设置中配置文本模型", http.StatusPreconditionRequired)
			return
		}

		targetURL, err := responsesURL(cfg)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		client, err := clientUpgrader.Upgrade(w, r, nil)
		if err != nil {
			log.Printf("upgrade 失败: %v", err)
			return
		}
		defer client.Close()

		header := http.Header{"Authorization": []string{"Bearer " + cfg.Key}}
		target, resp, err := websocket.DefaultDialer.Dial(targetURL, header)
		if err != nil {
			msg := "连接上游网关失败: " + err.Error()
			if resp != nil {
				msg += " (HTTP " + resp.Status + ")"
			}
			_ = client.WriteJSON(map[string]any{
				"type": "error",
				"error": map[string]any{"message": msg, "code": "upstream_connect_failed"},
			})
			return
		}
		defer target.Close()

		// 首个事件前注入当前配置的文本模型名，前端无需关心；
		// 连接锁定模型后改配置需重连（前端处理）
		injectModel := func(data []byte) []byte {
			var env struct {
				Type     string          `json:"type"`
				Response json.RawMessage `json:"response"`
			}
			if err := json.Unmarshal(data, &env); err != nil || env.Type != "response.create" || len(env.Response) == 0 {
				return data
			}
			var req map[string]json.RawMessage
			if err := json.Unmarshal(env.Response, &req); err != nil {
				return data
			}
			if _, exists := req["model"]; exists {
				return data // 客户端显式带了 model 就不动
			}
			req["model"] = mustJSON(cfg.TextModel)
			env.Response = mustJSON(req)
			out, err := json.Marshal(env)
			if err != nil {
				return data
			}
			return out
		}

		done := make(chan struct{})
		var once sync.Once
		closeDone := func() { once.Do(func() { close(done) }) }

		// 浏览器 -> 网关
		go func() {
			defer closeDone()
			for {
				msgType, data, err := client.ReadMessage()
				if err != nil {
					return
				}
				if err := target.WriteMessage(msgType, injectModel(data)); err != nil {
					return
				}
			}
		}()

		// 网关 -> 浏览器（原样转发）
		go func() {
			defer closeDone()
			for {
				msgType, data, err := target.ReadMessage()
				if err != nil {
					return
				}
				if err := client.WriteMessage(msgType, data); err != nil {
					return
				}
			}
		}()

		<-done
		_ = target.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "session ended"),
			time.Now().Add(time.Second))
		_ = client.WriteControl(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseNormalClosure, "session ended"),
			time.Now().Add(time.Second))
	}
}

func responsesURL(cfg Config) (string, error) {
	base := strings.TrimSpace(cfg.Base)
	if base == "" {
		return "", errors.New("平台地址为空")
	}
	if !strings.HasPrefix(base, "http://") && !strings.HasPrefix(base, "https://") {
		base = "https://" + base
	}
	if strings.HasPrefix(base, "https://") {
		base = "wss://" + strings.TrimPrefix(base, "https://")
	} else {
		base = "ws://" + strings.TrimPrefix(base, "http://")
	}
	return strings.TrimRight(base, "/") + "/v1/responses", nil
}

func mustJSON(v any) json.RawMessage {
	b, err := json.Marshal(v)
	if err != nil {
		return json.RawMessage("null")
	}
	return b
}
