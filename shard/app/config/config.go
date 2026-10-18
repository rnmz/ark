package config

import (
	"fmt"
	"log/slog"
	"os"
	"slices"
	"strings"

	"github.com/bytedance/sonic"
)

type AppConfig struct {
	// Common
	Mode     string `json:"mode,omitempty"`
	LogLevel string `json:"log_level,omitempty"`
	LogDir   string `json:"log_dir,omitempty"`

	// Server
	Port         int      `json:"port"`
	AllowedHosts []string `json:"allowed_hosts,omitempty"`
	MaxRangeSize int      `json:"max_range_size"`
	SecretKey    string   `json:"secret_key"`

	// File
	Path             string   `json:"path"`
	MaxFileSize      int      `json:"max_file_size"`
	BlockSize        int      `json:"block_size"`
	AllowedFileTypes []string `json:"allowed_file_types"`

	// DB
	DB       string   `json:"db"`
	Host     string   `json:"host"`
	Username string   `json:"username"`
	Password string   `json:"password"`
	DBModes  []string `json:"db_modes,omitempty"`
}

func ReadConfig(path string) (AppConfig, error) {
	var config AppConfig
	data, errFile := os.ReadFile(path)
	if errFile != nil {
		return AppConfig{}, errFile
	}

	if errSonic := sonic.Unmarshal(data, &config); errSonic != nil {
		return AppConfig{}, errSonic
	}
	if errValidate := validateConfig(&config); errValidate != nil {
		return AppConfig{}, errValidate
	}
	return config, nil
}

func applyDefaults(config *AppConfig) {
	if !slices.Contains([]string{"release", "debug"}, strings.ToLower(config.Mode)) {
		slog.Debug("[Config] Mode is not set or invalid, using default")
		config.Mode = "debug"
	}
	if !slices.Contains([]string{"debug", "info", "warn", "error"}, strings.ToLower(config.LogLevel)) {
		slog.Debug("[Config] LogLevel is not set or invalid, using default")
		config.LogLevel = "info"
	}
	if config.LogDir == "" {
		slog.Debug("[Config] LogDir is not set, using default")
		config.LogDir = "./logs"
	}
	if len(config.AllowedHosts) == 0 {
		slog.Debug("[Config] AllowedHosts is empty, defaulting to localhost")
		config.AllowedHosts = []string{"localhost", "127.0.0.1"}
	}
	if len(config.AllowedFileTypes) == 0 {
		slog.Debug("[Config] AllowedFileTypes is empty, defaulting to *")
		config.AllowedFileTypes = []string{"*"}
	}
	if len(config.DBModes) == 0 {
		slog.Debug("[Config] DBModes is empty")
	}
}

func validateConfig(config *AppConfig) error {
	applyDefaults(config)

	if config.Port <= 1024 || config.Port > 65535 {
		return AppConfigError{Reason: fmt.Sprintf("Port must be between 1025 and 65535: %d", config.Port)}
	}
	if config.MaxRangeSize <= 0 {
		return AppConfigError{Reason: fmt.Sprintf("MaxRangeSize must be greater than 0: %d", config.MaxRangeSize)}
	}
	if config.BlockSize <= 0 {
		return AppConfigError{Reason: fmt.Sprintf("BlockSize must be greater than 0: %d", config.BlockSize)}
	}
	if len(config.SecretKey) < 32 {
		return AppConfigError{Reason: fmt.Sprintf("SecretKey must be at least 32 bytes: %d", len(config.SecretKey))}
	}
	if config.Path == "" {
		return AppConfigError{Reason: "[Config] Path is empty"}
	}
	if config.MaxFileSize <= 0 {
		return AppConfigError{Reason: fmt.Sprintf("[Config] MaxFileSize must be greater than 0: %d", config.MaxFileSize)}
	}
	if config.DB == "" || config.Host == "" || config.Username == "" || config.Password == "" {
		return AppConfigError{Reason: "[DB] invalid config: missing credentials"}
	}

	return nil
}
