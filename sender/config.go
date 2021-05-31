package sender

import (
	"io/ioutil"
	"time"
)

// Config defines the config for file queue.
type Config struct {
	Logger             Logger
	SendInterval       time.Duration
	SendLimit          int
	UseMemoryFallback  bool
	FileWorkspace      string
	FleMaxCaraptedFile int
	ShowSuccessfulInfo bool
}

// ConfigDefault is the default config
var ConfigDefault = Config{
	UseMemoryFallback:  true,
	FileWorkspace:      "/tmp",
	FleMaxCaraptedFile: 1,
	ShowSuccessfulInfo: false,
}

// Helper function to set default values
func configDefault(config ...Config) Config {
	// Return default config if nothing provided
	if len(config) < 1 {
		return ConfigDefault
	}

	// Override default config
	cfg := config[0]

	if cfg.FileWorkspace == "" {
		cfg.FileWorkspace, _ = ioutil.TempDir("", "ballistic")
	}

	if cfg.SendLimit == 0 {
		cfg.SendLimit = 1
	}

	if cfg.SendInterval < 100*time.Millisecond {
		cfg.SendInterval = 100 * time.Millisecond
	}

	return cfg
}
