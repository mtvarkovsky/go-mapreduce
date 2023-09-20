package main

import (
	"bufio"
	"context"
	"fmt"
	"github.com/mtvarkovsky/go-mapreduce/pkg/api/grpc/pb"
	"github.com/mtvarkovsky/go-mapreduce/pkg/config"
	"github.com/mtvarkovsky/go-mapreduce/pkg/events"
	"github.com/mtvarkovsky/go-mapreduce/pkg/logger"
	"github.com/mtvarkovsky/go-mapreduce/pkg/utils"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/emptypb"
	sysLog "log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

type (
	fileScanner struct {
		log           logger.Logger
		wg            *sync.WaitGroup
		inputFilesDir string
		coordinator   pb.ServiceClient
		done          <-chan bool
		fileCache     map[string]any
	}

	mapWorker struct {
		log                  logger.Logger
		wg                   *sync.WaitGroup
		inputFilesDir        string
		intermediateFilesDir string
		coordinator          pb.ServiceClient
		consumer             events.Consumer
		done                 <-chan bool
	}

	reduceWorker struct {
		log                  logger.Logger
		wg                   *sync.WaitGroup
		intermediateFilesDir string
		outputFilesDir       string
		coordinator          pb.ServiceClient
		consumer             events.Consumer
		done                 <-chan bool
	}
)

func newFileScanner(
	log logger.Logger,
	wg *sync.WaitGroup,
	inputFilesDir string,
	coordinator pb.ServiceClient,
	done chan bool,
) *fileScanner {
	return &fileScanner{
		log:           log.Logger("FileScanner"),
		wg:            wg,
		inputFilesDir: inputFilesDir,
		coordinator:   coordinator,
		done:          done,
		fileCache:     make(map[string]any),
	}
}

func (fs *fileScanner) ScanFiles() {
	defer fs.wg.Done()

	if _, err := os.Stat(fs.inputFilesDir); os.IsNotExist(err) {
		fs.log.Errorf(err.Error())
	}

	ticker := time.NewTicker(time.Second * 10)
	err := fs.scanFiles()
	if err != nil {
		fs.log.Errorf(err.Error())
	}
	for {
		select {
		case <-ticker.C:
			err := fs.scanFiles()
			if err != nil {
				fs.log.Errorf(err.Error())
			}
		case <-fs.done:
			return
		}
	}
}

func (fs *fileScanner) scanFiles() error {
	dir, err := os.ReadDir(fs.inputFilesDir)
	if err != nil {
		return err
	}
	for _, entry := range dir {
		if entry.IsDir() {
			continue
		}
		file := filepath.Join(fs.inputFilesDir, entry.Name())
		if _, ok := fs.fileCache[file]; ok {
			continue
		}
		extension := filepath.Ext(file)
		if extension != ".txt" {
			continue
		}
		fs.fileCache[file] = nil
		fs.log.Infof("create map task for file=%s", file)
		_, err := fs.coordinator.CreateMapTask(
			context.Background(),
			&pb.NewMapTask{InputFile: file},
		)
		if err != nil {
			return err
		}
	}
	return nil
}

func newMapWorker(
	log logger.Logger,
	wg *sync.WaitGroup,
	inputFilesDir string,
	intermediateFilesDir string,
	coordinator pb.ServiceClient,
	consumer events.Consumer,
	done chan bool,
) *mapWorker {
	return &mapWorker{
		log:                  log.Logger("MapWorker"),
		wg:                   wg,
		inputFilesDir:        inputFilesDir,
		intermediateFilesDir: intermediateFilesDir,
		coordinator:          coordinator,
		consumer:             consumer,
		done:                 done,
	}
}

func (w *mapWorker) Start() {
	w.log.Infof("start map task worker")
	defer w.wg.Done()
	evnts, err := w.consumer.Consume()
	if err != nil {
		w.log.Errorf(err.Error())
	}
	for event := range evnts {
		if event.Type == events.MapTaskCreated {
			w.log.Infof("new map task received")
			go w.processMapTask()
		}
	}
}

func (w *mapWorker) processMapTask() {
	w.log.Infof("get map task")
	task, err := w.coordinator.GetMapTask(context.Background(), &emptypb.Empty{})
	if err != nil {
		w.log.Errorf(err.Error())
		return
	}
	w.log.Infof("got map task with id=%s, inputFile=%s", task.Id, task.InputFile)
	w.log.Infof("open input file=%s", task.InputFile)
	file, err := os.Open(task.InputFile)
	defer file.Close()
	if err != nil {
		w.handleErr(task, err)
		return
	}
	scanner := bufio.NewScanner(file)
	var outputFiles []string
	lineCount := 0
	for scanner.Scan() {
		line := scanner.Text()
		interMediateFileNameParts := strings.Split(task.InputFile, "/")
		interMediateFileName := fmt.Sprintf("%s/%d_%s", w.intermediateFilesDir, lineCount, interMediateFileNameParts[len(interMediateFileNameParts)-1])
		w.log.Infof("create intermediate file=%s", interMediateFileName)
		interMediateFile, err := os.Create(interMediateFileName)
		if err != nil {
			w.handleErr(task, err)
			return
		}
		_, err = interMediateFile.WriteString(line)
		if err != nil {
			w.handleErr(task, err)
			return
		}
		interMediateFile.Close()
		outputFiles = append(outputFiles, interMediateFileName)
		lineCount += 1
	}
	if scanner.Err() != nil {
		w.handleErr(task, err)
		return
	}
	w.log.Infof("report map task with id=%s, outputFiles=%s", task.Id, outputFiles)
	_, err = w.coordinator.ReportMapTaskResult(
		context.Background(),
		&pb.MapTaskResult{
			TaskId:      task.Id,
			OutputFiles: outputFiles,
		},
	)
	if err != nil {
		w.log.Errorf(err.Error())
		return
	}
}

func (w *mapWorker) handleErr(task *pb.MapTask, err error) {
	w.log.Errorf(err.Error())
	err = w.reportFail(task, err)
	if err != nil {
		w.log.Errorf(err.Error())
	}
	return
}

func (w *mapWorker) reportFail(task *pb.MapTask, err error) error {
	_, e := w.coordinator.ReportMapTaskResult(
		context.Background(),
		&pb.MapTaskResult{
			TaskId: task.Id,
			Error:  utils.Pointer(err.Error()),
		},
	)
	return e
}

func newReduceWorker(
	log logger.Logger,
	wg *sync.WaitGroup,
	intermediateFilesDir string,
	outputFilesDir string,
	coordinator pb.ServiceClient,
	consumer events.Consumer,
	done chan bool,
) *reduceWorker {
	return &reduceWorker{
		log:                  log.Logger("ReduceWorker"),
		wg:                   wg,
		intermediateFilesDir: intermediateFilesDir,
		outputFilesDir:       outputFilesDir,
		coordinator:          coordinator,
		consumer:             consumer,
		done:                 done,
	}
}

func (w *reduceWorker) Start() {
	w.log.Infof("start reduce task worker")
	defer w.wg.Done()
	evnts, err := w.consumer.Consume()
	if err != nil {
		w.log.Errorf(err.Error())
	}
	for event := range evnts {
		if event.Type == events.ReduceTaskCreated {
			w.log.Infof("new reduce task received")
			go w.processReduceTask()
		}
	}
}

func (w *reduceWorker) processReduceTask() {
	w.log.Infof("get reduce task")
	task, err := w.coordinator.GetReduceTask(context.Background(), &emptypb.Empty{})
	if err != nil {
		w.log.Errorf(err.Error())
		return
	}
	w.log.Infof("got reduce task with id=%s, inputFiles=%s", task.Id, task.InputFiles)
	wordCount := 0
	for _, inputFile := range task.InputFiles {
		w.log.Infof("open input file=%s", inputFile)
		file, err := os.Open(inputFile)
		if err != nil {
			w.handleErr(task, err)
			return
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			words := strings.Split(line, " ")
			wordCount += len(words)
		}
		if scanner.Err() != nil {
			w.handleErr(task, err)
			return
		}
		file.Close()
	}
	outputFileName := fmt.Sprintf("%s/%s", w.outputFilesDir, strings.Split(task.InputFiles[0], "_")[1])
	w.log.Infof("create output file=%s", outputFileName)
	outputFile, err := os.Create(outputFileName)
	defer outputFile.Close()
	if err != nil {
		w.handleErr(task, err)
		return
	}
	_, err = outputFile.WriteString(fmt.Sprintf("%d\n", wordCount))
	if err != nil {
		w.handleErr(task, err)
		return
	}
	w.log.Infof("report reduce task with id=%s, outputFile=%s", task.Id, outputFileName)
	_, err = w.coordinator.ReportReduceTaskResult(
		context.Background(),
		&pb.ReduceTaskResult{
			TaskId:     task.Id,
			OutputFile: outputFileName,
		},
	)
	if err != nil {
		w.log.Errorf(err.Error())
		return
	}
}

func (w *reduceWorker) handleErr(task *pb.ReduceTask, err error) {
	w.log.Errorf(err.Error())
	err = w.reportFail(task, err)
	if err != nil {
		w.log.Errorf(err.Error())
	}
	return
}

func (w *reduceWorker) reportFail(task *pb.ReduceTask, err error) error {
	_, e := w.coordinator.ReportReduceTaskResult(
		context.Background(),
		&pb.ReduceTaskResult{
			TaskId: task.Id,
			Error:  utils.Pointer(err.Error()),
		},
	)
	return e
}

func main() {
	if len(os.Args) < 7 {
		sysLog.Fatalf(
			"usage: wordcount " +
				"{input_files_dir} " +
				"{intermediate_files_dir} " +
				"{output_files_dir} " +
				"{coordinator_connection_string} " +
				"{rabbitmq_connection_string} " +
				"{rabbitmq_queue}",
		)
	}

	inputFilesDir := os.Args[1]
	intermediateFilesDir := os.Args[2]
	outputFilesDir := os.Args[3]
	coordinatorConnStr := os.Args[4]
	rabbitMQConnStr := os.Args[5]
	rabbitMQQueue := os.Args[6]

	consumerCfg := config.RabbitMQ{
		URL:              rabbitMQConnStr,
		QueueName:        rabbitMQQueue,
		EventsBufferSize: 10,
	}

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

	log := logger.NewZapLogger("Wordcount")

	grpcConn, err := grpc.Dial(
		coordinatorConnStr,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Fatalf(err.Error())
	}

	coordinatorClient := pb.NewServiceClient(grpcConn)

	consumer, err := events.NewRabbitMQConsumer(consumerCfg, log)
	if err != nil {
		log.Fatalf(err.Error())
	}

	wg := &sync.WaitGroup{}

	wg.Add(3)

	doneFileScanner := make(chan bool)
	fs := newFileScanner(log, wg, inputFilesDir, coordinatorClient, doneFileScanner)

	doneMapWorker := make(chan bool)
	mw := newMapWorker(log, wg, inputFilesDir, intermediateFilesDir, coordinatorClient, consumer, doneMapWorker)

	doneReduceWorker := make(chan bool)
	rw := newReduceWorker(log, wg, inputFilesDir, outputFilesDir, coordinatorClient, consumer, doneReduceWorker)

	go fs.ScanFiles()
	go mw.Start()
	go rw.Start()

	<-ctx.Done()

	consumer.Close()

	doneFileScanner <- true
	doneMapWorker <- true
	doneReduceWorker <- true

	wg.Wait()
	grpcConn.Close()
}
