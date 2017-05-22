package main

import (
	"fmt"
	"net/http"

	"io"
	"io/ioutil"

	"github.com/Financial-Times/go-fthealth/v1a"
	log "github.com/Sirupsen/logrus"
	"strings"
	"errors"
	"github.com/gorilla/mux"
	"encoding/json"
	"github.com/Financial-Times/keyphrase-rw-neo4j/keyphrase"
)

const (
	YESTERDAY = 86400
	ONE_WEEK_AGO = 604800
	ONE_MONTH_AGO = 2629743
	SIX_MONTHS_AGO = 6 * ONE_MONTH_AGO
)

type keyphraseHandlers struct {
	keyphraseDriver keyphrase.Service
	vulcanAddr      string
	timePeriod 	string
}

func (hh *keyphraseHandlers) kafkaProxyHealthCheck() v1a.Check {
	return v1a.Check{
		BusinessImpact:   "Unable to connect to kafka proxy",
		Name:             "Check connectivity to kafka-proxy and presence of configured topic which is a parameter in hieradata for this service",
		PanicGuide:       "https://sites.google.com/a/ft.com/universal-publishing/ops-guides/concept-ingestion",
		Severity:         1,
		TechnicalSummary: `Cannot connect to kafka-proxy. If this check fails, check that cluster is up and running, proxy is healthy and configured topic is present on the queue.`,
		Checker:          hh.checkCanConnectToKafkaProxy,
	}
}
func (hh *keyphraseHandlers) neo4jHealthCheck() v1a.Check {
	return v1a.Check{
		BusinessImpact:   "Unable to access neo4j data; cannot process requests",
		Name:             "Check connectivity to Neo4j --neo-url is part of the service_args in hieradata for this service",
		PanicGuide:       "TODO - write panic guide",
		Severity:         1,
		TechnicalSummary: "Cannot connect to Neo4j a instance with at least one keyphrase loaded in it",
		Checker:          hh.checkCanConnectToNeo4j,
	}
}

func (hh *keyphraseHandlers) checkCanConnectToNeo4j() (string, error) {
	err := hh.keyphraseDriver.Check()
	if err == nil {
		return "Can connect to Neo4j instance", err
	}
	return "Error connecting to Neo4j", err
}

func (hh *keyphraseHandlers) checkCanConnectToKafkaProxy() (string, error) {
	_, err := checkProxyConnection(hh.vulcanAddr)
	if err != nil {
		return fmt.Sprintf("Healthcheck: Error reading request body: %v", err.Error()), err
	}
	return "", nil
}

func checkProxyConnection(vulcanAddr string) ([]byte, error) {
	//check if proxy is running and topic is present
	req, err := http.NewRequest("GET", vulcanAddr+"/topics", nil)
	if err != nil {
		log.Errorf("Creating kafka-proxy check resulted in error: %v", err.Error())
		return nil, err
	}
	req.Host = "kafka"
	resp, err := httpClient.Do(req)
	if err != nil {
		log.Errorf("Healthcheck: Execution of kafka-proxy GET request resulted in error: %v", err.Error())
	}
	defer func() {
		if resp == nil {
			return
		}
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()
	if resp == nil {
		return nil, fmt.Errorf("Connecting to kafka-proxy was unsuccessful.")
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("Connecting to kafka-proxy was unsuccessful. Status was %v", resp.StatusCode)
	}
	return ioutil.ReadAll(resp.Body)
}

func (hh *keyphraseHandlers) ping(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "pong")
}

//goodToGo returns a 503 if the healthcheck fails - suitable for use from varnish to check availability of a node
func (hh *keyphraseHandlers) goodToGo(writer http.ResponseWriter, req *http.Request) {
	if _, err := hh.checkCanConnectToKafkaProxy(); err != nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
	if _, err := hh.checkCanConnectToNeo4j(); err != nil {
		writer.WriteHeader(http.StatusServiceUnavailable)
		return
	}
}

// buildInfoHandler - This is a stop gap and will be added to when we can define what we should display here
func (hh *keyphraseHandlers) buildInfoHandler(w http.ResponseWriter, req *http.Request) {
	fmt.Fprintf(w, "build-info")
}

func jsonMessage(msgText string) []byte {
	return []byte(fmt.Sprintf(`{"message":"%s"}`, msgText))
}

func (hh *keyphraseHandlers) putAnnotations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	if !strings.Contains(strings.ToLower(r.Header.Get("Content-Type")), "application-json") {
		err := errors.New("Http header 'Content-Type' is not 'application-json', this is a JSON API")
		log.Error(err)
		http.Error(w, string(jsonMessage(err.Error())), http.StatusBadRequest)
	}
	vars := mux.Vars(r)
	uuid := vars["uuid"]
	decoder := json.NewDecoder(r.Body)
	annotation, _, err := hh.keyphraseDriver.DecodeJSON(decoder)
	if err != nil {
		msg := fmt.Sprintf("Error (%v) parsing annotation request", err)
		log.Info(msg)
		writeJSONError(w, msg, http.StatusBadRequest)
		return
	}
	err = hh.keyphraseDriver.Write(uuid, annotation)
	if err != nil {
		msg := fmt.Sprintf("Error creating annotations (%v)", err)
		log.Error(msg)
		writeJSONError(w, msg, http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusCreated)
	w.Write([]byte(jsonMessage(fmt.Sprintf("Annotations for content %s created", uuid))))
	return
}

// GetAnnotations returns a view of the annotations written - it is NOT the public annotations API, and
// the response format should be consistent with the PUT request body format
func (hh *keyphraseHandlers) GetAnnotations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	vars := mux.Vars(r)
	uuid := vars["uuid"]

	if uuid == "" {
		writeJSONError(w, "uuid required", http.StatusBadRequest)
		return
	}
	annotations, found, err := hh.keyphraseDriver.Read(uuid)
	if err != nil {
		msg := fmt.Sprintf("Error getting annotations (%v)", err)
		log.Error(msg)
		writeJSONError(w, msg, http.StatusServiceUnavailable)
		return
	}
	if !found {
		writeJSONError(w, fmt.Sprintf("No annotations found for content with uuid %s.", uuid), http.StatusNotFound)
		return
	}
	Jason, _ := json.Marshal(annotations)
	log.Debugf("Annotations for content (uuid:%s): %s\n", Jason)
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(annotations)
}

//
func (hh *keyphraseHandlers) GetPopularDay(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	statistics, err := hh.keyphraseDriver.GetPopular(YESTERDAY)
	fmt.Printf("Result is %s\n", statistics)
	if err != nil {
		msg := fmt.Sprintf("Error getting annotations (%v)", err)
		log.Error(msg)
		writeJSONError(w, msg, http.StatusServiceUnavailable)
		return
	}
	//if !found {
	//	writeJSONError(w, fmt.Sprintf("No annotations found for content with uuid %s.", uuid), http.StatusNotFound)
	//	return
	//}
	//Jason, _ := json.Marshal(annotations)
	//log.Debugf("Annotations for content (uuid:%s): %s\n", Jason)
	//w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	//w.WriteHeader(http.StatusOK)
	//json.NewEncoder(w).Encode(annotations)
}

// DeleteAnnotations will delete all the annotations for a piece of content
func (hh *keyphraseHandlers) DeleteAnnotations(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")

	vars := mux.Vars(r)
	uuid := vars["uuid"]

	if uuid == "" {
		writeJSONError(w, "uuid required", http.StatusBadRequest)
		return
	}
	found, err := hh.keyphraseDriver.Delete(uuid)
	if err != nil {
		writeJSONError(w, err.Error(), http.StatusServiceUnavailable)
		return
	}
	if !found {
		writeJSONError(w, fmt.Sprintf("No annotations found for content with uuid %s.", uuid), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=UTF-8")
	w.WriteHeader(http.StatusNoContent)
	w.Write([]byte(jsonMessage(fmt.Sprintf("Annotations for content %s deleted", uuid))))
}

func (hh *keyphraseHandlers) CountAnnotations(w http.ResponseWriter, r *http.Request) {
	count, err := hh.keyphraseDriver.Count()

	w.Header().Add("Content-Type", "application/json")

	if err != nil {
		log.Errorf("Error on read=%v\n", err)
		writeJSONError(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

	enc := json.NewEncoder(w)

	if err := enc.Encode(count); err != nil {
		log.Errorf("Error on json encoding=%v\n", err)
		writeJSONError(w, err.Error(), http.StatusServiceUnavailable)
		return
	}

}

func writeJSONError(w http.ResponseWriter, errorMsg string, statusCode int) {
	w.WriteHeader(statusCode)
	fmt.Fprintln(w, fmt.Sprintf("{\"message\": \"%s\"}", errorMsg))
}

//func DecodeJSON(dec *json.Decoder) (interface{}, error) {
//	ann := keyphrase.Annotation{}
//	err := dec.Decode(&ann)
//	return ann, err
//}
