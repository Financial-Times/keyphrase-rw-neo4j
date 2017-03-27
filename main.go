package main

import (
	"net/http"
	"os"
	"strings"
	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"net"
	"github.com/Financial-Times/go-fthealth/v1a"
	log "github.com/Sirupsen/logrus"
	"github.com/jawher/mow.cli"
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/Financial-Times/keyphrase-rw-neo4j/keyphrase"
	"github.com/Financial-Times/base-ft-rw-app-go/baseftrwapp"
	"github.com/gorilla/mux"
	"github.com/rcrowley/go-metrics"
	"github.com/Financial-Times/http-handlers-go/httphandlers"
	status "github.com/Financial-Times/service-status-go/httphandlers"
)

var httpClient = http.Client{
	Transport: &http.Transport{
		MaxIdleConnsPerHost: 128,
		Dial: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).Dial,
	},
}

func main() {
	log.SetLevel(log.InfoLevel)
	app := cli.App("keyphrase rw-neo4j", "A microservice that consumes ConceptSuggestion messages from Kafka, extracts all KeyPhrases and writes them to Neo4J")
	port := app.String(cli.StringOpt{
		Name:   "port",
		Value:  "1234",
		Desc:   "Port to listen on",
		EnvVar: "PORT",
	})
	vulcanAddr := app.String(cli.StringOpt{
		Name:   "vulcan_addr",
		Value:  "http://localhost:8080",
		Desc:   "Vulcan address for routing requests",
		EnvVar: "VULCAN_ADDR",
	})
	consumerGroupID := app.String(cli.StringOpt{
		Name:   "consumer_group_id",
		Value:  "KeyphraseRwNeo4jGroup",
		Desc:   "Kafka group id used for message consuming.",
		EnvVar: "GROUP_ID",
	})
	consumerQueue := app.String(cli.StringOpt{
		Name:   "consumer_queue_id",
		Value:  "kafka",
		Desc:   "The kafka queue id",
		EnvVar: "QUEUE_ID",
	})
	consumerOffset := app.String(cli.StringOpt{
		Name:   "consumer_offset",
		Value:  "largest",
		Desc:   "Kafka read offset.",
		EnvVar: "OFFSET"})
	consumerAutoCommitEnable := app.Bool(cli.BoolOpt{
		Name:   "consumer_autocommit_enable",
		Value:  true,
		Desc:   "Enable autocommit for small messages.",
		EnvVar: "COMMIT_ENABLE"})
	topic := app.String(cli.StringOpt{
		Name:   "topic",
		Value:  "ConceptSuggestions",
		Desc:   "Kafka topic subscribed to",
		EnvVar: "TOPIC"})
	throttle := app.Int(cli.IntOpt{
		Name:   "throttle",
		Value:  1000,
		Desc:   "Throttle",
		EnvVar: "THROTTLE"})
	neoURL := app.String(cli.StringOpt{
		Name:   "neo-url",
		Value:  "http://localhost:7474/db/data",
		Desc:   "neo4j endpoint URL",
		EnvVar: "NEO_URL",
	})
	batchSize := app.Int(cli.IntOpt{
		Name:   "batch-size",
		Value:  1024,
		Desc:   "Maximum number of statements to execute per batch",
		EnvVar: "BATCH_SIZE",
	})
	graphiteTCPAddress := app.String(cli.StringOpt{
		Name:   "graphite-tcp-address",
		Value:  "",
		Desc:   "Graphite TCP address, e.g. graphite.ft.com:2003. Leave as default if you do NOT want to output to graphite (e.g. if running locally)",
		EnvVar: "GRAPHITE_TCP_ADDRESS",
	})
	graphitePrefix := app.String(cli.StringOpt{
		Name:   "graphite-prefix",
		Value:  "",
		Desc:   "Prefix to use. Should start with content, include the environment, and the host name. e.g. coco.pre-prod.special-reports-rw-neo4j.1",
		EnvVar: "GRAPHITE_PREFIX",
	})
	logMetrics := app.Bool(cli.BoolOpt{
		Name:   "log-metrics",
		Value:  false,
		Desc:   "Whether to log metrics. Set to true if running locally and you want metrics output",
		EnvVar: "LOG_METRICS",
	})

	app.Action = func() {
		consumerConfig := queueConsumer.QueueConfig{
			Addrs:                strings.Split(*vulcanAddr, ","),
			Group:                *consumerGroupID,
			Queue:                *consumerQueue,
			Topic:                *topic,
			Offset:               *consumerOffset,
			AutoCommitEnable:     *consumerAutoCommitEnable,
			ConcurrentProcessing: true,
		}

		conf := neoutils.DefaultConnectionConfig()
		conf.BatchSize = *batchSize
		db, err := neoutils.Connect(*neoURL, conf)

		if err != nil {
			log.Errorf("Could not connect to neo4j, error=[%s]\n", err)
		}

		keyphraseDriver := keyphrase.NewCypherKeyphraseService(db)
		keyphraseDriver.Initialise()

		processors := processors{time.NewTicker(time.Second / time.Duration(*throttle)), httpClient, keyphraseDriver}

		baseftrwapp.OutputMetricsIfRequired(*graphiteTCPAddress, *graphitePrefix, *logMetrics)

		consumer := queueConsumer.NewConsumer(consumerConfig, processors.readMessage, &httpClient)

		var wg sync.WaitGroup
		wg.Add(1)

		go func() {
			consumer.Start()
			wg.Done()
		}()

		go runServer(keyphraseDriver, *vulcanAddr, *port)

		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)

		<-ch
		log.Println("Shutting down application...")

		consumer.Stop()
		wg.Wait()

		log.Println("Application closing")
	}
	app.Run(os.Args)
}

func runServer(keyphraseDriver keyphrase.Service, vulcanAddr string, port string) {
	keyphraseHandlers := keyphraseHandlers{keyphraseDriver:keyphraseDriver, vulcanAddr:vulcanAddr}

	router := router(keyphraseHandlers)

	http.HandleFunc(status.PingPath, status.PingHandler)
	http.HandleFunc(status.PingPathDW, status.PingHandler)
	http.HandleFunc(status.BuildInfoPath, status.BuildInfoHandler)
	http.HandleFunc(status.BuildInfoPathDW, status.BuildInfoHandler)
	log.Infof("keyphrase-rw-neo4j-go-app will listen on port: %s", port)
	http.Handle("/", router)

	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Unable to start server: %v\n", err)
	}

}
func router(handlers keyphraseHandlers) http.Handler {
	serviceRouter := mux.NewRouter()
	var checks []v1a.Check = []v1a.Check{handlers.kafkaProxyHealthCheck(), handlers.neo4jHealthCheck()}

	serviceRouter.HandleFunc("/__health", v1a.Handler("Keyphrase healthchecks", "Checks connectivity to kafka proxy and Neo4j", checks...))
	serviceRouter.HandleFunc("/__gtg", handlers.goodToGo)
	serviceRouter.HandleFunc("/content/{uuid}/keyphrase/annotations", handlers.GetAnnotations).Methods("GET")
	serviceRouter.HandleFunc("/content/{uuid}/keyphrase/annotations", handlers.putAnnotations).Methods("PUT")
	serviceRouter.HandleFunc("/content/{uuid}/keyphrase/annotations", handlers.DeleteAnnotations).Methods("DELETE")
	serviceRouter.HandleFunc("/content/keyphrase/annotations/__count", handlers.CountAnnotations).Methods("GET")

	var monitoringRouter http.Handler = serviceRouter
	monitoringRouter = httphandlers.TransactionAwareRequestLoggingHandler(log.StandardLogger(), monitoringRouter)
	monitoringRouter = httphandlers.HTTPMetricsHandler(metrics.DefaultRegistry, monitoringRouter)

	return monitoringRouter
}
