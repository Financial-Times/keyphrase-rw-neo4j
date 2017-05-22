package main

import (
	queueConsumer "github.com/Financial-Times/message-queue-gonsumer/consumer"
	log "github.com/Sirupsen/logrus"
	"net/http"
	"time"
	"github.com/Financial-Times/keyphrase-rw-neo4j/keyphrase"
	"encoding/json"
	"errors"
	"strings"
)

var keyphraseOntology = "http://www.ft.com/ontology/extraction/KeyPhrase"

type processors struct {
	ticker 		*time.Ticker
	httpClient	http.Client
	keyphraseDriver keyphrase.Service
}

func (p processors) readMessage(msg queueConsumer.Message) {
	<-p.ticker.C

	if msg.Headers["Origin-System-Id"] != "concept-suggestor" || msg.Headers["Content-Type"] != "application/json" {
		return
	}
	//TODO do something with transID???
	//transId := msg.Headers["X-Request-Id"]

	err := processMessage(msg.Body, p.keyphraseDriver)
	if err != nil {
		log.Errorf("%v", err)
	}
}

func processMessage(msgBody string, kps keyphrase.Service) error {
	//TODO name this something "better"
	msgSuggestions := keyphrase.Suggestion{}
	//jmb, _ := json.Marshal(msgBody)
	//fmt.Printf("Marshalled body is %s\n", jmb)
	err := json.Unmarshal([]byte(msgBody), &msgSuggestions)
	if err != nil {
		return errors.New("Could not unmarshall json with error " + err.Error())
	}
	processAnnotations(msgSuggestions, kps)
	return nil

}

func processAnnotations(suggestion keyphrase.Suggestion, kps keyphrase.Service) {
	//var onlyKeyphrase []keyphrase.Annotation
	for _, annotation := range suggestion.Suggestions {
		for _, annotationType := range annotation.Thing.Types {
			if strings.Contains(annotationType, keyphraseOntology) {
				go kps.Write(suggestion.Uuid, annotation)
			}
		}
	}
}

