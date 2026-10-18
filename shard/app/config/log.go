package config

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"
)

func InitLogger(level string, dir string) (*os.File, error) {
	var lvl slog.Level
	switch level {
	case "debug":
		lvl = slog.LevelDebug
	case "info":
		lvl = slog.LevelInfo
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	}

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("[Log] create dir %q: %w", dir, err)
	}

	file, err := createLogFile(dir)
	if err != nil {
		return nil, err
	}

	h := slog.NewTextHandler(file, &slog.HandlerOptions{
		Level: lvl,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if len(groups) == 0 && a.Key == slog.TimeKey {
				if t, ok := a.Value.Any().(time.Time); ok {
					a.Value = slog.StringValue(t.Format("2006-01-02 15:04:05.000"))
				}
			}
			return a
		},
	})
	slog.SetDefault(slog.New(h))

	return file, nil
}

func createLogFile(dir string) (*os.File, error) {
	name := fmt.Sprintf("app_%s_%d.log", time.Now().Format("20060102_150405"), os.Getpid())
	path := filepath.Join(dir, name)

	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, fmt.Errorf("[Log] open file %q: %w", path, err)
	}
	return f, nil
}
