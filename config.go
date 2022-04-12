package googlebigquery

import (
	"fmt"
	"log"
)

const (
	// ConfigKeyAWSAccessKeyID is the config name for AWS access secret key
	ConfigProjectID = "srcProjectID"

	// ConfigKeyAWSSecretAccessKey is the config name for AWS secret access key
	ConfigDatasetID = "srcDatasetID" // nolint:gosec // false positive

	// ConfigKeyAWSRegion is the config name for AWS region
	ConfigTableID = "srcTableID"

	ConfigServiceAccount = "serviceAccount"
)

// Config represents configuration needed for S3
type Config struct {
	ConfigProjectID      string
	ConfigDatasetID      string
	ConfigTableID        string
	ConfigServiceAccount string
}

// SourceConfig is config for source
type SourceConfig struct {
	Config Config
}

func ParseSourceConfig(cfg map[string]string) (SourceConfig, error) {
	err := checkEmpty(cfg)
	if err != nil {
		log.Println("Empty config found:", err)
		return SourceConfig{}, err
	}

	config := Config{
		ConfigServiceAccount: cfg[ConfigServiceAccount],
		ConfigProjectID:      cfg[ConfigProjectID],
		ConfigDatasetID:      cfg[ConfigDatasetID],
		ConfigTableID:        cfg[ConfigTableID]}

	return SourceConfig{
		Config: config,
	}, nil
}

func checkEmpty(cfg map[string]string) error {
	if len(cfg) == 0 {
		return fmt.Errorf("empty config found")
	}
	return nil
}
