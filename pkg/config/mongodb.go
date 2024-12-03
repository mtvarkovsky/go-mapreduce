package config

import (
	"fmt"
	"strings"

	"github.com/spf13/viper"
)

type MongoDB struct {
	URI    string `mapstructure:"URI"`
	DBName string `mapstructure:"DB_NAME"`
}

func (cfg *MongoDB) LoadConfig(name string) (*MongoDB, error) {
	viper.AddConfigPath("./configs")
	viper.SetConfigName(name)
	viper.SetConfigType("yaml")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(`.`, `_`))

	err := viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("can't read config for MongoDB from=%s: %w", name, err)
	}

	err = viper.Unmarshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshall config for MongoDB from=%s: %w", name, err)
	}

	return cfg, nil
}
