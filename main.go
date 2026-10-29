package main

import (
	"embed"
	"encoding/json"
	"flag"
	"io/fs"
	"log"
	"net/http"
	"strconv"
)

//go:embed web
var webFS embed.FS

func main() {
	addr := flag.String("addr", "0.0.0.0:8080", "HTTP 监听地址")
	dbPath := flag.String("db", "voicechat.db", "SQLite 数据库文件路径")
	flag.Parse()

	store, err := OpenStore(*dbPath)
	if err != nil {
		log.Fatalf("打开数据库失败: %v", err)
	}
	defer store.Close()

	http.HandleFunc("/api/config", handleConfig(store))
	http.HandleFunc("/api/usage", handleUsage(store))
	http.HandleFunc("/ws", handleWSProxy(store))
	http.HandleFunc("/ws-responses", handleWSResponses(store))

	static, _ := fs.Sub(webFS, "web")
	http.Handle("/", http.FileServer(http.FS(static)))

	log.Printf("语音对话服务已启动: http://%s  (数据库: %s)", *addr, *dbPath)
	if err := http.ListenAndServe(*addr, nil); err != nil {
		log.Fatal(err)
	}
}

// ---------- /api/config ----------

func handleConfig(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			cfg, err := store.GetConfig()
			if err == nil {
				cfg.Key = maskKey(cfg.Key) // Key 只留在服务端，前端拿到脱敏值
			}
			writeJSON(w, cfg, err)
		case http.MethodPut:
			var cfg Config
			if err := json.NewDecoder(r.Body).Decode(&cfg); err != nil {
				http.Error(w, "请求体不是合法 JSON: "+err.Error(), http.StatusBadRequest)
				return
			}
			if err := store.SaveConfig(cfg); err != nil {
				http.Error(w, "保存失败: "+err.Error(), http.StatusInternalServerError)
				return
			}
			writeJSON(w, map[string]string{"status": "ok"}, nil)
		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	}
}

// ---------- /api/usage ----------

func handleUsage(store *Store) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		limit := 50
		if v := r.URL.Query().Get("limit"); v != "" {
			if n, err := strconv.Atoi(v); err == nil && n > 0 && n <= 500 {
				limit = n
			}
		}
		rows, err := store.RecentUsage(limit)
		if err != nil {
			http.Error(w, "查询失败: "+err.Error(), http.StatusInternalServerError)
			return
		}
		totals, err := store.UsageTotals()
		if err != nil {
			http.Error(w, "统计失败: "+err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]any{"rows": rows, "totals": totals}, nil)
	}
}

func writeJSON(w http.ResponseWriter, v any, err error) {
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	_ = json.NewEncoder(w).Encode(v)
}

// maskKey 把 sk-xxxxxxx 显示为 sk-***xxxx，只露首尾各几个字符。
func maskKey(key string) string {
	if key == "" {
		return ""
	}
	if len(key) <= 8 {
		return "***"
	}
	return key[:5] + "***" + key[len(key)-4:]
}
