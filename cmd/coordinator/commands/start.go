package commands

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/mtvarkovsky/go-mapreduce/pkg/app"
	"github.com/mtvarkovsky/go-mapreduce/pkg/config"

	"github.com/spf13/cobra"
)

var startCmd = &cobra.Command{
	Use:   "start",
	Short: "Start the application",
	RunE: func(c *cobra.Command, args []string) error {
		ctx, closer := context.WithCancel(context.Background())
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			select {
			case <-ch:
				closer()
			case <-ctx.Done():
			}
		}()

		mongoCfg := &config.MongoDB{}
		mongoCfg, err := mongoCfg.LoadConfig("mongodb.yaml")
		if err != nil {
			return err
		}

		rabbitCfg := &config.RabbitMQ{}
		rabbitCfg, err = rabbitCfg.LoadConfig("rabbitmq.yaml")
		if err != nil {
			return err
		}

		coordinatorCfg := &config.Coordinator{}
		coordinatorCfg, err = coordinatorCfg.LoadConfig("coordinator.yaml")
		if err != nil {
			return err
		}

		return app.RunCoordinator(ctx, *coordinatorCfg, *rabbitCfg, *mongoCfg)
	},
}
