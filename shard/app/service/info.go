package info

import (
	"ark/shard/app/config"
	"time"
)

var timeStart = time.Now()

type ServerInfo struct {
	FreeSpace  int // MB
	FullSpace  int // MB
	Memory     int // MB
	Uptime     time.Duration
	DisksSMART map[string]any
	Name       string
}

func GetInfo(cfg config.App) ServerInfo {
	return ServerInfo{
		FreeSpace:  1000, // Todo: read from disk
		FullSpace:  1000, // Todo: read from disk
		Memory:     1000, // Toddo: empty
		Uptime:     time.Since(timeStart),
		DisksSMART: map[string]any{}, // todo: use rnmz/gotasker
		Name:       cfg.Name,
	}
}
