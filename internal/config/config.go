// Package config 装配 Cline2api 的运行配置：.env 加载 + 上游常量 + 状态目录。
//
// 状态目录规则与 Python 版对齐（Python: dirname(abspath(__file__))）：
// 凭据、免费清单缓存、请求日志、dumps/ 全部落在状态目录。
// 部署时把 exe 放进原 Cline2api 目录（或设 CLINE2API_STATE_DIR 指向它），
// 即可无缝接管 Python 版的 credentials.json / free_cache.json。
package config

import (
	"bufio"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	APIBase     = "https://api.cline.bot"
	ChatPath    = "/api/v1/chat/completions"
	ModelsPath  = "/api/v1/ai/cline/recommended-models"
	UA          = "Cline2api/1.0-go"
	ListenHost  = "127.0.0.1"
	DefaultPort = 8381
)

type Config struct {
	APIKey       string
	Port         int
	StateDir     string // credentials.json / free_cache.json / request.log / dumps/ 所在
	DumpOn       bool
	DumpDir      string
	DumpMaxFiles int
}

// StateFilePath 返回状态目录下的文件路径。
func (c *Config) StateFilePath(name string) string { return filepath.Join(c.StateDir, name) }

// Load 读取配置。优先级：环境变量 > .env > 默认值。
// .env 从状态目录和工作目录两处尝试（与 python-dotenv 的 load_dotenv 语义近似）。
func Load() *Config {
	c := &Config{
		Port:         DefaultPort,
		DumpOn:       true,
		DumpMaxFiles: 200,
	}

	// 状态目录：env 显式指定 > exe 所在目录
	c.StateDir = os.Getenv("CLINE2API_STATE_DIR")
	if c.StateDir == "" {
		if exe, err := os.Executable(); err == nil {
			c.StateDir = filepath.Dir(exe)
		} else {
			wd, _ := os.Getwd()
			c.StateDir = wd
		}
	}

	// .env：状态目录优先，其次工作目录；两处都有的键先到先得
	for _, dir := range []string{c.StateDir, mustWd()} {
		loadDotEnv(filepath.Join(dir, ".env"))
	}

	c.APIKey = strings.TrimSpace(os.Getenv("API_KEY"))
	if p, err := strconv.Atoi(strings.TrimSpace(os.Getenv("PORT"))); err == nil && p > 0 {
		c.Port = p
	}
	if v := strings.TrimSpace(os.Getenv("DUMP")); v == "0" {
		c.DumpOn = false
	}
	if v := strings.TrimSpace(os.Getenv("DUMP_MAX_FILES")); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			c.DumpMaxFiles = n
		}
	}
	c.DumpDir = strings.TrimSpace(os.Getenv("DUMP_DIR"))
	if c.DumpDir == "" {
		c.DumpDir = filepath.Join(c.StateDir, "dumps")
	}
	return c
}

func mustWd() string {
	wd, err := os.Getwd()
	if err != nil {
		return "."
	}
	return wd
}

// loadDotEnv 极简 .env 解析：KEY=VALUE 行，# 注释，不展开变量，不覆盖已有 env。
func loadDotEnv(path string) {
	f, err := os.Open(path)
	if err != nil {
		return
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(k)
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if k != "" && os.Getenv(k) == "" {
			os.Setenv(k, v)
		}
	}
}
