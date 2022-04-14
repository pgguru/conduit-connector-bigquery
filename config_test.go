package googlebigquery

import (
	"testing"
)

func TestParseNoConfig(t *testing.T) {

	cfg := map[string]string{}
	_, err := ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got no error")
	}

}

func TestParseSourceConfigAllConfigPresent(t *testing.T) {

	cfg := map[string]string{}
	cfg[ConfigServiceAccount] = "test"
	cfg[ConfigProjectID] = "test"
	cfg[ConfigDatasetID] = "test"

	_, err := ParseSourceConfig(cfg)
	if err != nil {
		t.Errorf("parse source config, got error %v", err)
	}

}

func TestParseSourceConfigPartialConfig(t *testing.T) {

	cfg := map[string]string{}
	delete(cfg, ConfigServiceAccount)
	cfg[ConfigProjectID] = "test"
	cfg[ConfigDatasetID] = "test"

	_, err := ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got error %v", err)
	}

	cfg[ConfigServiceAccount] = "test"
	delete(cfg, ConfigProjectID)
	cfg[ConfigDatasetID] = "test"

	_, err = ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got error %v", err)
	}

	cfg[ConfigServiceAccount] = "test"
	cfg[ConfigProjectID] = "test"
	delete(cfg, ConfigDatasetID)

	_, err = ParseSourceConfig(cfg)
	if err == nil {
		t.Errorf("parse source config, got error %v", err)
	}

}

func TestSpecification(t *testing.T) {
	Specification()
}
