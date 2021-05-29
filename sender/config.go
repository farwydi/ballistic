package sender

import "io/ioutil"

// Config defines the config for file queue.
type Config struct {
	Logger             Logger
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

	return cfg
}
