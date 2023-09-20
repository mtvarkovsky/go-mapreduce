# Wordcount

This is an example application that uses https://github.com/mtvarkovsky/go-mapreduce.

It counts words in an input file and produces an output file with the result.

In order to run it you'll first need to build the coordinator docker image:
```bash
$ cd ../..
```
```bash
$ make docker-build-coordinator
```

After that you'll need to return to the example app folder and build the wordcount docker image:
```bash
$ cd examples/wordcount
```
```bash
$ make docker-build-wordcount
```

Now you can run a dockerized version of the app:
```bash
$ make docker-run-wordcount
```

Kick back, relax, and watch the logs while the application is starting.

## How to use this app

This app is designed to work with three folders:
- input - folder for input .txt files
- intermediate - folder for intermediate results of map tasks
- output - folder for output files

When the app is running, it scans the input folder for new .txt files every N seconds.

If a new file is found a new map task is created.

While files are scanned, the application actively listens for new task events from the coordinator.

Each event type (map task created, reduce task created) is handled by a dedicated worker who performs word counting logic and reports task results.

I've provided a default test file, but you can test it yourself by placing a .txt file in the input folder.

The map tasks worker splits .txt file by lines to intermediate files.

The reduce tasks worker counts words in each intermediate file and accumulates the result in the output folder.

