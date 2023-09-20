package app

import (
	"context"
	"fmt"
	"github.com/mtvarkovsky/go-mapreduce/pkg/api/grpc"
	"github.com/mtvarkovsky/go-mapreduce/pkg/api/grpc/pb"
	"github.com/mtvarkovsky/go-mapreduce/pkg/config"
	"github.com/mtvarkovsky/go-mapreduce/pkg/events"
	"github.com/mtvarkovsky/go-mapreduce/pkg/ids"
	"github.com/mtvarkovsky/go-mapreduce/pkg/logger"
	"github.com/mtvarkovsky/go-mapreduce/pkg/repository"
	"github.com/mtvarkovsky/go-mapreduce/pkg/service"
	"net"
	"sync"
)

func RunCoordinator(
	ctx context.Context,
	coordinatorCfg config.Coordinator,
	rabbitMQCfg config.RabbitMQ,
	mongoDBCfg config.MongoDB,
) error {
	log := logger.NewZapLogger("CoordinatorApp")
	log.Infof("starting coordinator app")

	repo, err := repository.NewMongoDBTasksRepository(ctx, mongoDBCfg, log)
	if err != nil {
		log.Fatalf("can't get repository: (%s)", err.Error())
		return err
	}

	producer, err := events.NewRabbitMQProducer(rabbitMQCfg, log)
	if err != nil {
		log.Fatalf("can't get producer: (%s)", err.Error())
		return err
	}

	idGen, err := ids.NewULIDIDGenerator()
	if err != nil {
		log.Fatalf("can't get id generator: (%s)", err.Error())
		return err
	}

	coordinator := service.NewCoordinator(ctx, coordinatorCfg, repo, idGen, producer, log)

	go coordinator.TaskFlusher()
	go coordinator.MapTasksRescheduler()
	go coordinator.ReduceTasksRescheduler()

	coordinatorGrpcServer := grpc.NewCoordinator(coordinator, log)
	grpcServer := grpc.NewServer()

	log.Infof("start grpc server at localhost:%d", coordinatorCfg.GrpcPort)
	lisGrpc, err := net.Listen("tcp", fmt.Sprintf(":%d", coordinatorCfg.GrpcPort))
	if err != nil {
		log.Fatalf("failed to listen: (%s)", err.Error())
	}
	pb.RegisterServiceServer(grpcServer, coordinatorGrpcServer)
	go func() {
		if err = grpcServer.Serve(lisGrpc); err != nil {
			log.Fatalf("failed to serve: (%s)", err.Error())
		}
		log.Infof("grpc server at localhost:%d started", coordinatorCfg.GrpcPort)
	}()

	<-ctx.Done()
	log.Infof("stop coordinator app")

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		grpcServer.GracefulStop()
		lisGrpc.Close()
		wg.Done()
	}()

	wg.Wait()

	err = producer.Close(ctx)
	if err != nil {
		log.Warnf("can't close producer: (%s)", err.Error())
	}

	return nil
}
