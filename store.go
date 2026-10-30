package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"

	_ "modernc.org/sqlite" // 纯 Go SQLite 驱动，无需 cgo
)

// Config 是唯一的配置行（id=1）。Key 只存在服务端数据库，前端通过 /api/config
// 读取时会拿到脱敏值，浏览器全程接触不到完整 Key。
type Config struct {
	Base      string   `json:"base"`
	Key       string   `json:"key"`
	Model     string   `json:"model"`      // Realtime 模型
	TextModel string   `json:"text_model"` // Responses WS 文本模型
	Voice     string   `json:"voice"`
	Voices    []string `json:"voices"` // 音色候选列表（可编辑，随上游渠道维护），DB 中存 JSON 数组
	SysPrompt string   `json:"sys_prompt"`
}

// defaultVoices 是未配置时的默认候选（OpenAI Realtime GA 内置音色）。
// 上游不提供音色枚举接口，列表只能按文档维护，换渠道时在设置页改。
var defaultVoices = []string{"alloy", "ash", "ballad", "cedar", "coral", "echo", "sage", "shimmer", "verse"}

// UsageRow 是一轮 response.done 的用量快照。
type UsageRow struct {
	ID           int64     `json:"id"`
	Model        string    `json:"model"`
	InputTokens  int64     `json:"input_tokens"`
	OutputTokens int64     `json:"output_tokens"`
	TotalTokens  int64     `json:"total_tokens"`
	InputAudio   int64     `json:"input_audio"`
	OutputAudio  int64     `json:"output_audio"`
	CreatedAt    time.Time `json:"created_at"`
}

type Store struct {
	db *sql.DB
}

func OpenStore(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1) // SQLite 单写者，串行化避免 database is locked
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	_, err := s.db.Exec(`
CREATE TABLE IF NOT EXISTS config (
	id         INTEGER PRIMARY KEY CHECK (id = 1),
	base       TEXT NOT NULL DEFAULT '',
	key        TEXT NOT NULL DEFAULT '',
	model      TEXT NOT NULL DEFAULT '',
	text_model TEXT NOT NULL DEFAULT '',
	voice      TEXT NOT NULL DEFAULT 'alloy',
	sys_prompt TEXT NOT NULL DEFAULT '',
	updated_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS usage (
	id            INTEGER PRIMARY KEY AUTOINCREMENT,
	model         TEXT NOT NULL DEFAULT '',
	input_tokens  INTEGER NOT NULL DEFAULT 0,
	output_tokens INTEGER NOT NULL DEFAULT 0,
	total_tokens  INTEGER NOT NULL DEFAULT 0,
	input_audio   INTEGER NOT NULL DEFAULT 0,
	output_audio  INTEGER NOT NULL DEFAULT 0,
	created_at    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_usage_created ON usage(created_at DESC);`)
	if err != nil {
		return err
	}
	// 旧库升级：补 text_model 列
	if _, err := s.db.Exec(`ALTER TABLE config ADD COLUMN text_model TEXT NOT NULL DEFAULT ''`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			return err
		}
	}
	// 旧库升级：补 voices 列（JSON 数组文本）
	if _, err := s.db.Exec(`ALTER TABLE config ADD COLUMN voices TEXT NOT NULL DEFAULT '[]'`); err != nil {
		if !strings.Contains(err.Error(), "duplicate column") {
			return err
		}
	}
	return nil
}

func (s *Store) GetConfig() (Config, error) {
	var c Config
	var voicesJSON string
	var updatedAt int64
	err := s.db.QueryRow(
		`SELECT base, key, model, text_model, voice, voices, sys_prompt, updated_at FROM config WHERE id = 1`,
	).Scan(&c.Base, &c.Key, &c.Model, &c.TextModel, &c.Voice, &voicesJSON, &c.SysPrompt, &updatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		// 未配置过：给默认值（模型名按 GA 系列填，用户可在设置里改）
		return Config{Model: "gpt-realtime", TextModel: "gpt-5.2", Voice: "alloy", Voices: defaultVoices}, nil
	}
	if err != nil {
		return c, err
	}
	c.Voices = parseVoices(voicesJSON)
	return c, nil
}

func (s *Store) SaveConfig(c Config) error {
	voicesJSON, err := json.Marshal(c.Voices)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(`
INSERT INTO config (id, base, key, model, text_model, voice, voices, sys_prompt, updated_at)
VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(id) DO UPDATE SET
	base = excluded.base,
	key = CASE WHEN excluded.key = '' THEN config.key ELSE excluded.key END,
	model = excluded.model,
	text_model = excluded.text_model,
	voice = excluded.voice,
	voices = CASE WHEN excluded.voices = 'null' THEN config.voices ELSE excluded.voices END,
	sys_prompt = excluded.sys_prompt,
	updated_at = excluded.updated_at`,
		c.Base, c.Key, c.Model, c.TextModel, c.Voice, string(voicesJSON), c.SysPrompt, time.Now().Unix())
	return err
}

// parseVoices 容错解析 voices JSON 列：空或非法时回退默认列表。
func parseVoices(s string) []string {
	s = strings.TrimSpace(s)
	if s == "" || s == "[]" || s == "null" {
		return defaultVoices
	}
	var v []string
	if err := json.Unmarshal([]byte(s), &v); err != nil || len(v) == 0 {
		return defaultVoices
	}
	return v
}

func (s *Store) AddUsage(u UsageRow) error {
	_, err := s.db.Exec(`
INSERT INTO usage (model, input_tokens, output_tokens, total_tokens, input_audio, output_audio, created_at)
VALUES (?, ?, ?, ?, ?, ?, ?)`,
		u.Model, u.InputTokens, u.OutputTokens, u.TotalTokens, u.InputAudio, u.OutputAudio, time.Now().Unix())
	return err
}

func (s *Store) RecentUsage(limit int) ([]UsageRow, error) {
	rows, err := s.db.Query(`
SELECT id, model, input_tokens, output_tokens, total_tokens, input_audio, output_audio, created_at
FROM usage ORDER BY id DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []UsageRow
	for rows.Next() {
		var u UsageRow
		var ts int64
		if err := rows.Scan(&u.ID, &u.Model, &u.InputTokens, &u.OutputTokens, &u.TotalTokens,
			&u.InputAudio, &u.OutputAudio, &ts); err != nil {
			return nil, err
		}
		u.CreatedAt = time.Unix(ts, 0)
		out = append(out, u)
	}
	return out, rows.Err()
}

func (s *Store) UsageTotals() (UsageRow, error) {
	var u UsageRow
	var ts int64
	err := s.db.QueryRow(`
SELECT COUNT(*),
	COALESCE(SUM(input_tokens), 0),
	COALESCE(SUM(output_tokens), 0),
	COALESCE(SUM(total_tokens), 0),
	COALESCE(SUM(input_audio), 0),
	COALESCE(SUM(output_audio), 0),
	COALESCE(MAX(created_at), 0)
FROM usage`).Scan(&u.ID, &u.InputTokens, &u.OutputTokens, &u.TotalTokens,
		&u.InputAudio, &u.OutputAudio, &ts)
	u.CreatedAt = time.Unix(ts, 0)
	if errors.Is(err, sql.ErrNoRows) {
		return UsageRow{}, nil
	}
	return u, err
}
