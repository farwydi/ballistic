package file

// Config defines the config for file queue.
type Config struct {
	Workspace  string
	MaxHistory int
}

// ConfigDefault is the default config
var ConfigDefault = Config{
	Workspace:  "/tmp",
	MaxHistory: 3,
}

// Helper function to set default values
func configDefault(config ...Config) Config {
	// Return default config if nothing provided
	if len(config) < 1 {
		return ConfigDefault
	}

	// Override default config
	cfg := config[0]

	if cfg.Workspace == "" {
		cfg.Workspace = ConfigDefault.Workspace
	}

	if cfg.MaxHistory == 0 {
		cfg.MaxHistory = ConfigDefault.MaxHistory
	}

	return cfg
}
