package googlebigquery

import (
	"errors"
	"fmt"
	"log"
)

const (
	// ConfigProjectID is the config projectID
	ConfigProjectID = "srcProjectID"

	// ConfigDatasetID is the dataset ID
	ConfigDatasetID = "srcDatasetID"

	// ConfigTableID is the tableID
	ConfigTableID = "srcTableID"

	// ConfigServiceAccount path to service account key
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

	if _, ok := cfg[ConfigServiceAccount]; !ok {
		return SourceConfig{}, errors.New("service account can't be blank")
	}

	if _, ok := cfg[ConfigProjectID]; !ok {
		return SourceConfig{}, errors.New("project ID can't be blank")
	}

	if _, ok := cfg[ConfigDatasetID]; !ok {
		return SourceConfig{}, errors.New("dataset ID can't be blank")
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
