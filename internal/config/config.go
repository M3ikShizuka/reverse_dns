package config

import (
	"fmt"
	"github.com/go-playground/validator/v10"
	"github.com/ory/viper"
)

const (
	defHTTPAddr = "0.0.0.0"
	defHTTPPort = 3000
)

type Config struct {
	HTTP      HTTPConfig `mapstructure:"http"`
	DNSClient DNSClient  `mapstructure:"dns_client"`
	DB        Database   `mapstructure:"database"`
}

type HTTPConfig struct {
	Proto      string `mapstructure:"proto" validate:"required"`
	ListenAddr string `mapstructure:"listen_addr" validate:"required"`
	Port       int32  `mapstructure:"port" validate:"required"`
	Host       string
	HostURL    string
}

type DNSClient struct {
	DSN string `mapstructure:"dsn" validate:"required"`
}

type Database struct {
	DSN     string      `mapstructure:"dsn" validate:"required"`
	DBName  string      `mapstructure:"db_name" validate:"required"`
	Collect Collections `mapstructure:"collections"`
}

type Collections struct {
	DNSRecord         string `mapstructure:"dns_record" validate:"required"`
	DNSRecordLifetime string `mapstructure:"dns_record_lifetime" validate:"required"`
	DNSNonHistorical  string `mapstructure:"dns_non_historical" validate:"required"`
}

func NewConfig() *Config {
	return &Config{}
}

func (config *Config) Init(configPath string) error {
	// Set default values.
	config.setDefault()

	// Load settings from config file.
	if err := config.parseConfig(configPath); err != nil {
		return err
	}

	// Unmarshal config to struct.
	if err := config.unmarshal(); err != nil {
		return err
	}

	// Env
	config.getEnv()

	// Init composite fields.
	config.initCompositeFields()

	return nil
}

func (config *Config) setDefault() {
	viper.SetDefault("http.listen_addr", defHTTPAddr)
	viper.SetDefault("http.port", defHTTPPort)
}

func (config *Config) parseConfig(configPath string) error {
	viper.SetConfigType("yml")
	viper.SetConfigFile(configPath)
	viper.AutomaticEnv()
	//viper.SetEnvPrefix("SERVICE_ACCOUNT") // will be uppercased automatically

	if err := viper.ReadInConfig(); err != nil {
		return err
	}

	return nil
}

func (config *Config) unmarshal() error {
	//// Unmarshal all but not check.
	if err := viper.Unmarshal(config); err != nil {
		return err
	}

	// Check initialization of all important fields.
	validate := validator.New()
	if err := validate.Struct(config); err != nil {
		return fmt.Errorf("initializing the configuration: Missing required attributes %w\n", err)
	}

	return nil
}

func (config *Config) getEnv() {
	// TODO: add another

	if envar := viper.GetString("SERVICE_ACCOUNT_DSN"); envar != "" {
		config.DB.DSN = envar
	}
}

func (config *Config) initCompositeFields() {
	config.HTTP.Host = fmt.Sprintf("%s:%d", config.HTTP.ListenAddr, config.HTTP.Port)
	config.HTTP.HostURL = fmt.Sprintf("%s://%s", config.HTTP.Proto, config.HTTP.Host)

	// TODO: add another
}
