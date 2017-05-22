package keyphrase

import (
	"github.com/Financial-Times/neo-utils-go/neoutils"
	"github.com/jmcvetta/neoism"
	"errors"
	log "github.com/Sirupsen/logrus"
	"fmt"
	"time"
	"regexp"
	"encoding/json"
)

var lifecycle string = "keyphrase"
var uuidExtractRegex = regexp.MustCompile(".*/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$")

type service struct {
	conn neoutils.NeoConnection
}

func NewCypherKeyphraseService(conn neoutils.NeoConnection) service {
	return service{conn}
}

type Service interface {
	Write(contentUUID string, thing interface{}) (err error)
	Read(contentUUID string) (thing interface{}, found bool, err error)
	Delete(contentUUID string) (found bool, err error)
	Check() (err error)
	Count() (int, error)
	DecodeJSON(dec *json.Decoder) (interface{}, string, error)
	Initialise() error
	GetPopular(timePeriod int) (thing interface{}, err error)
}

func (s service) Write(contentUUID string, thing interface{}) (error) {
	annotationToWrite := thing.(Annotation)

	if contentUUID == "" {
		return errors.New("Content uuid is required")
	}
	if err := validateAnnotations(annotationToWrite); err != nil {
		log.Warnf("Validation of supplied annotations failed")
		return err
	}

	err := s.createKeyphrase(annotationToWrite.Thing)
	if err != nil {
		return err
	}

	//queries := append([]*neoism.CypherQuery{}, buildDeleteQuery(contentUUID, false))

	var statements = []string{}

	query, err := createAnnotationQuery(contentUUID, annotationToWrite)
	if err != nil {
		return err
	}
	statements = append(statements, query.Statement)
	queries := append([]*neoism.CypherQuery{}, query)

	log.Infof("Updated Annotations for content uuid: %s", contentUUID)
	log.Debugf("For update, ran statements: %+v", statements)

	return s.conn.CypherBatch(queries)
}

func (s service) Read(contentUuid string) (interface{}, bool, error) {
	results := []Annotation{}

	readQuery := &neoism.CypherQuery{
		Statement: `MATCH (c:Thing{uuid{uuid}})-[rel]->(kp:Keyphrase)
				WITH c, cc, rel, {id:cc.uuid,prefLabel:cc.prefLabel,types:labels(cc),predicate:type(rel)} as thing,
				collect(
					{scores:[
						{scoringSystem:'%s', value:rel.relevanceScore},
						{scoringSystem:'%s', value:rel.confidenceScore}],
					agentRole:rel.annotatedBy,
					atTime:rel.annotatedDate}) as provenances
				RETURN thing, provenances ORDER BY thing.id`,
		Parameters: map[string]interface{}{
			"uuid": contentUuid,
		},
		Result: &results,
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{readQuery})

	if err != nil {
		return Annotation{}, false, err
	}

	if len(results) == 0 {
		return Annotation{}, false, nil
	}


	return results[0], true, nil
}

func (s service) Delete(contentUUID string) (bool, error) {

	query := buildDeleteQuery(contentUUID, true)

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})

	stats, err := query.Stats()
	if err != nil {
		return false, err
	}

	return stats.ContainsUpdates, err
}

// Check tests neo4j by running a simple cypher query
func (s service) Check() error {
	return neoutils.Check(s.conn)
}

func (s service) Count() (int, error) {
	results := []struct {
		Count int `json:"c"`
	}{}

	query := &neoism.CypherQuery{
		Statement: `MATCH (kp:Keyphrase)<-[r]-(t:Thing)
                WHERE r.lifecycle = {lifecycle}
                OR r.lifecycle IS NULL
                RETURN count(r) as c`,
		Parameters: neoism.Props{"lifecycle": lifecycle},
		Result:     &results,
	}

	err := s.conn.CypherBatch([]*neoism.CypherQuery{query})

	if err != nil {
		return 0, err
	}

	return results[0].Count, nil
}

func (s service) Initialise() error {
	return s.conn.EnsureConstraints(map[string]string{
		"Thing":          "uuid",
		"Keyphrase": 	  "uuid"})
}

func createAnnotationRelationship() (statement string) {
	stmt := `
                MERGE (concept:Thing{uuid:{conceptID}})
                MERGE (content:Thing{uuid:{contentID}})
                MERGE (ces:Identifier:CesIdentifier{value:{conceptID}})
                MERGE (ces)-[:IDENTIFIES]->(concept)
                MERGE (content)-[pred:MENTIONS]->(concept)
                SET pred={annProps}
          `
	statement = fmt.Sprintf(stmt)
	return statement
}
func (s service) createKeyphrase(thing Thing) (error) {
	keyphraseId, _ := extractUUIDFromURI(thing.ID)
	prefLabel := thing.PrefLabel

	deletePreviousDetailsQuery := &neoism.CypherQuery{
		Statement: `MATCH (t:Thing {uuid:{uuid}})
		REMOVE t:Concept:Keyphrase
		SET t.uuid={uuid}`,
		Parameters: map[string]interface{}{
			"uuid": keyphraseId,
		},
	}

	queries := append([]*neoism.CypherQuery{}, deletePreviousDetailsQuery)

	createkKeyphraseQuery := &neoism.CypherQuery{
		Statement: `MERGE (n:Thing{uuid:{keyphraseId}})
			    SET n:Concept:Keyphrase, n.uuid={uuid}, n.prefLabel={prefLabel}`,
		Parameters: map[string]interface{}{
		"keyphraseId": keyphraseId,
		"uuid":      keyphraseId,
		"prefLabel": prefLabel,
		},
	}

	queries = append(queries, createkKeyphraseQuery)

	err := s.conn.CypherBatch(queries)
	if err != nil {
		return err
	}
	return nil
}

func createAnnotationQuery(contentUUID string, ann Annotation) (*neoism.CypherQuery, error) {
	query := neoism.CypherQuery{}
	thingID, err := extractUUIDFromURI(ann.Thing.ID)
	if err != nil {
		return nil, err
	}

	var prov provenance
	params := map[string]interface{}{}
	params["lifecycle"] = lifecycle

	if len(ann.Provenances) >= 1 {
		prov = ann.Provenances[0]
		annotatedBy, annotatedDateEpoch, relevanceScore, confidenceScore, supplied, err := extractDataFromProvenance(&prov)

		if err != nil {
			log.Infof("ERROR=%s", err)
			return nil, err
		}

		if supplied == true {
			if annotatedBy != "" {
				params["annotatedBy"] = annotatedBy
			}
			if prov.AtTime != "" {
				params["annotatedDateEpoch"] = annotatedDateEpoch
				params["annotatedDate"] = prov.AtTime
			}
			params["relevanceScore"] = relevanceScore
			params["confidenceScore"] = confidenceScore
		}
	}

	query.Statement = createAnnotationRelationship()
	query.Parameters = map[string]interface{}{
		"contentID":       contentUUID,
		"conceptID":       thingID,
		"annProps":        params,
	}
	return &query, nil
}

func extractDataFromProvenance(prov *provenance) (string, int64, float64, float64, bool, error) {
	if len(prov.Scores) == 0 {
		return "", -1, -1, -1, false, nil
	}
	var annotatedBy string
	var annotatedDateEpoch int64
	var confidenceScore, relevanceScore float64
	var err error
	if prov.AgentRole != "" {
		annotatedBy, err = extractUUIDFromURI(prov.AgentRole)
	}
	if prov.AtTime != "" {
		annotatedDateEpoch, err = convertAnnotatedDateToEpoch(prov.AtTime)
	}
	relevanceScore, confidenceScore, err = extractScores(prov.Scores)

	if err != nil {
		return "", -1, -1, -1, true, err
	}
	return annotatedBy, annotatedDateEpoch, relevanceScore, confidenceScore, true, nil
}

func extractUUIDFromURI(uri string) (string, error) {
	result := uuidExtractRegex.FindStringSubmatch(uri)
	if len(result) == 2 {
		return result[1], nil
	}
	return "", fmt.Errorf("Couldn't extract uuid from uri %s", uri)
}

func convertAnnotatedDateToEpoch(annotatedDateString string) (int64, error) {
	datetimeEpoch, err := time.Parse(time.RFC3339, annotatedDateString)

	if err != nil {
		return 0, err
	}

	return datetimeEpoch.Unix(), nil
}

func extractScores(scores []score) (float64, float64, error) {
	var relevanceScore, confidenceScore float64
	for _, score := range scores {
		scoringSystem := score.ScoringSystem
		value := score.Value
		switch scoringSystem {
		case relevanceScoringSystem:
			relevanceScore = value
		case confidenceScoringSystem:
			confidenceScore = value
		}
	}
	return relevanceScore, confidenceScore, nil
}

func buildDeleteQuery(contentUUID string, includeStats bool) *neoism.CypherQuery {
	var statement string

	//TODO hard-coded verification:
	//WE STILL NEED THIS UNTIL EVERYTHNG HAS A LIFECYCLE PROPERTY!
	// -> necessary for brands - which got written by content-api with isClassifiedBy relationship, and should not be deleted by annotations-rw
	// -> so far brands are the only v2 concepts which have isClassifiedBy relationship; as soon as this changes: implementation needs to be updated
	switch {

	default:
		statement = `	OPTIONAL MATCH (:Thing{uuid:{contentID}})-[r]->(t:Keyphrase)
                         		DELETE r`
	}

	query := neoism.CypherQuery{
		Statement: statement,
		Parameters: neoism.Props{"contentID": contentUUID},
		IncludeStats: includeStats}
	return &query
}

func validateAnnotations(annotation Annotation) error {
	//TODO - for consistency, we should probably just not create the annotation?
	annotation.Thing.PrefLabel
	if annotation.Thing.ID == "" {
		return ValidationError{fmt.Sprintf("Concept uuid missing for annotation %+v", annotation)}
	}
	return nil
}

//ValidationError is thrown when the annotations are not valid because mandatory information is missing
type ValidationError struct {
	Msg string
}

func (v ValidationError) Error() string {
	return v.Msg
}


func (s service) DecodeJSON(dec *json.Decoder) (interface{}, string, error) {
	ann := Annotation{}
	err := dec.Decode(&ann)
	return ann, "", err
}

func (s service) GetPopular(timePeriod int) (interface{}, error) {
	results := []PopularKeyphrase{}

	readQuery := &neoism.CypherQuery{
		Statement: `MATCH (c:Content)-[a]->(k:Keyphrase)
			    WITH COUNT(DISTINCT a) as count, k, c
			    WHERE c.publishedDateEpoch > {timePeriod} AND k.prefLabel =~ '[a-z]*'
			    WITH k.prefLabel as prefLabel, SUM(count) AS sum
			    RETURN prefLabel, sum ORDER BY sum DESC`,
		Parameters: map[string]interface{}{
			"timePeriod": timePeriod,
		},
		Result: &results,
	}

	fmt.Printf("Result is %s\n", &results)

	err := s.conn.CypherBatch([]*neoism.CypherQuery{readQuery})
	fmt.Printf("Result is %s\n", &results)

	if err != nil {
		return Annotation{}, err
	}

	if len(results) == 0 {
		return Annotation{}, nil
	}


	return results[0], nil
}

//func mapToResponseFormat(ann *annotation) {
//	ann.Thing.ID = mapper.IDURL(ann.Thing.ID)
//	// We expect only ONE provenance - provenance value is considered valid even if the AgentRole is not specified. See: v1 - isClassifiedBy
//	for idx := range ann.Provenances {
//		if ann.Provenances[idx].AgentRole != "" {
//			ann.Provenances[idx].AgentRole = mapper.IDURL(ann.Provenances[idx].AgentRole)
//		}
//	}
//}

