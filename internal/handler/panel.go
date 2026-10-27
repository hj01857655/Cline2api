// 面板与 /admin/api/overview：内置一份 panel.html（//go:embed），状态目录里有
// 同名文件时优先用用户的——内置版只是让 /admin 开箱即用，不依赖手动安装步骤。
// overview 是面板数据源：凭据快照（绝不外泄 token 本体）+ 免费池状态 + 模型清单。
package handler

import (
	_ "embed"
	"fmt"
	"net/http"
	"os"
	"time"
)

//go:embed panel.html
var panelBuiltin []byte

// servePanel 输出面板 HTML：状态目录 panel.html > 内置版。
func (s *Server) servePanel(w http.ResponseWriter) {
	raw, err := os.ReadFile(s.Cfg.StateFilePath("panel.html"))
	if err != nil || len(raw) == 0 {
		raw = panelBuiltin
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	_, _ = w.Write(raw)
}

// adminOverview 面板概览数据：凭据快照 + 免费池状态 + 免费模型清单。
// 凭据文件缺失/损坏不是错误响应——照常 200，把 notice 带给面板展示。
func (s *Server) adminOverview(w http.ResponseWriter, r *http.Request) {
	credInfo := map[string]any{
		"valid": false, "provider": "", "expiresAt": int64(0),
		"remain_text": "—", "notice": "",
	}
	if info, err := s.Cred.Load(); err != nil {
		credInfo["notice"] = err.Error()
	} else {
		credInfo["provider"] = info.Provider
		credInfo["expiresAt"] = info.ExpiresAt
		credInfo["remain_text"] = remainText(info.ExpiresAt)
		if _, terr := s.Cred.Token(); terr != nil {
			credInfo["notice"] = terr.Error()
		} else {
			credInfo["valid"] = true
		}
	}

	ids := s.Free.IDs() // 空时内部已 quick 拉一次
	models := make([]any, 0, len(ids))
	for _, id := range ids {
		models = append(models, map[string]any{"id": id, "owned_by": "cline-free"})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service":    "Cline2api",
		"credential": credInfo,
		"local_auth": s.Cfg.APIKey != "",
		"free_pool":  s.Free.Status(),
		"models":     models,
	})
}

// remainText 到期时刻的可读剩余量（零值/已过期如实标注）。
func remainText(exp int64) string {
	if exp <= 0 {
		return "未知"
	}
	d := time.Until(time.Unix(exp, 0))
	if d <= 0 {
		return "已过期"
	}
	if d < time.Hour {
		return fmt.Sprintf("%.0f 分钟", d.Minutes())
	}
	return fmt.Sprintf("%.1f 小时", d.Hours())
}
