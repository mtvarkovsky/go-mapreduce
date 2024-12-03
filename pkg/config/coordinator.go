package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/spf13/viper"
)

type Coordinator struct {
	RescheduleInProgressTasksAfter time.Duration `mapstructure:"RESCHEDULE_IN_PROGRESS_TASKS_AFTER"`
	CheckForTasksToRescheduleEvery time.Duration `mapstructure:"CHECK_FOR_TASKS_TO_RESCHEDULE_EVERY"`
	AutoFlushTasksEvery            time.Duration `mapstructure:"AUTO_FLUSH_TASKS_EVERY"`
	RepoType                       string
	GrpcPort                       int `mapstructure:"GRPC_PORT"`
}

const (
	MongoDBRepo = "mongodb"
)

func (cfg *Coordinator) LoadConfig(name string) (*Coordinator, error) {
	viper.AddConfigPath("./configs")
	viper.SetConfigName(name)
	viper.SetConfigType("yaml")
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(`.`, `_`))

	err := viper.ReadInConfig()
	if err != nil {
		return nil, fmt.Errorf("can't read config for Coordinator from=%s: %w", name, err)
	}

	err = viper.Unmarshal(cfg)
	if err != nil {
		return nil, fmt.Errorf("can't unmarshall config for Coordinator from=%s: %w", name, err)
	}

	return cfg, nil
}
