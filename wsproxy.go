package main

import (
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// upstreamEvent 只解析代理转发时需要旁路观察的字段，其余内容原样透传。
type upstreamEvent struct {
	Type string `json:"type"`
	Response *struct {
		Model string `json:"model"`
		Usage *struct {
			TotalTokens       int64 `json:"total_tokens"`
			InputTokens       int64 `json:"input_tokens"`
			OutputTokens      int64 `json:"output_tokens"`
			InputTokenDetails  struct {
				AudioTokens int64 `json:"audio_tokens"`
			} `json:"input_token_details"`
			OutputTokenDetails struct {
				AudioTokens int64 `json:"audio_tokens"`
			} `json:"output_token_details"`
		} `json:"usage"`
	} `json:"response"`
}

var clientUpgrader = websocket.Upgrader{
	// 本地工具：只服务本机浏览器，同源页面直接放行
	CheckOrigin: func(r *http.Request) bool { return true },
}

// handleWSProxy 把浏览器 WebSocket 代理到网关的 /v1/realtime。
// 鉴权在后端完成（Authorization 头），Key 不经过浏览器。
func handleWSProxy(store *Store) http.HandlerFunc {
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

		targetURL, err := realtimeURL(cfg)
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
				if err := target.WriteMessage(msgType, data); err != nil {
					return
				}
			}
		}()

		// 网关 -> 浏览器（旁路统计用量后原样转发）
		go func() {
			defer closeDone()
			for {
				msgType, data, err := target.ReadMessage()
				if err != nil {
					return
				}
				recordUsage(store, data)
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

func realtimeURL(cfg Config) (string, error) {
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
	base = strings.TrimRight(base, "/")
	model := url.QueryEscape(cfg.Model)
	return base + "/v1/realtime?model=" + model, nil
}

// recordUsage 从 response.done 事件里提取 usage 并落库。
func recordUsage(store *Store, data []byte) {
	var ev upstreamEvent
	if err := json.Unmarshal(data, &ev); err != nil {
		return // 非法 JSON 原样转发即可，不因旁路统计断流
	}
	if ev.Type != "response.done" || ev.Response == nil || ev.Response.Usage == nil {
		return
	}
	u := ev.Response.Usage
	model := ev.Response.Model
	if model == "" {
		if cfg, err := store.GetConfig(); err == nil {
			model = cfg.Model
		}
	}
	row := UsageRow{
		Model:        model,
		InputTokens:  u.InputTokens,
		OutputTokens: u.OutputTokens,
		TotalTokens:  u.TotalTokens,
		InputAudio:   u.InputTokenDetails.AudioTokens,
		OutputAudio:  u.OutputTokenDetails.AudioTokens,
	}
	if err := store.AddUsage(row); err != nil {
		log.Printf("记录用量失败: %v", err)
	}
}
