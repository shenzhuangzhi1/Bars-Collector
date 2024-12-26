package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// DatabaseConfig defines the database connection properties
type DatabaseConfig struct {
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	DBName   string `yaml:"dbname"`
	SSLMode  string `yaml:"sslmode"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
}

// Config defines the overall configuration structure
type Config struct {
	Database DatabaseConfig `yaml:"database"`
}

// LoadConfig reads and parses the YAML configuration file
func LoadConfig(filepath string) (*Config, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %v", err)
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	var config Config
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("failed to decode config file: %v", err)
	}

	return &config, nil
}

// GetPostgresConnectionString builds the PostgreSQL connection string
func GetPostgresConnectionString(config *DatabaseConfig) string {
	connStr := fmt.Sprintf(
		"user=%s password=%s dbname=%s sslmode=%s host=%s port=%d",
		config.User,
		config.Password,
		config.DBName,
		config.SSLMode,
		config.Host,
		config.Port,
	)
	return connStr
}
