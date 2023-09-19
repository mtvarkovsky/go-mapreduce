package config

import (
	"fmt"
	"github.com/spf13/viper"
	"strings"
)

type RabbitMQ struct {
	URL              string `mapstructure:"URL"`
	QueueName        string `mapstructure:"QUEUE_NAME"`
	EventsBufferSize int    `mapstructure:"EVENTS_BUFFER_SIZE"`
}

func (cfg *RabbitMQ) LoadConfig(name string) (*RabbitMQ, error) {
	viper.AddConfigPath("./configs")
	viper.SetConfigName(name)
	viper.SetConfigType("yaml")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(`.`, `_`))

	err := viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("can't read config for RabbitMQ from=%s: %w", name, err)
	}

	err = viper.Unmarshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshall config for RabbitMQ from=%s: %w", name, err)
	}

	return cfg, nil
}
