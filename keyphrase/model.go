package keyphrase

type annotations []Annotation

type Suggestion struct {
	Suggestions annotations  `json:"suggestions"`
	Uuid string		  `json:"uuid"`
}
//TODO Not sure this is right...
//Annotation is the main struct used to create and return structures
type Annotation struct {
	Thing       Thing        `json:"thing,omitempty"`
	Provenances []provenance `json:"provenances,omitempty"`
}

//TODO had to make public to be accessible to processor.Go should move processor? or public is cool?
//Thing represents a concept being linked to
type Thing struct {
	ID        string   `json:"id,omitempty"`
	//TODO prefLabel can be different dependinng upon context. Delete and re-write for now. possible label list in the future?
	PrefLabel string   `json:"prefLabel,omitempty"`
	Types     []string `json:"types,omitempty"`
	Predicate string   `json:"predicate,omitempty"`
}

//Provenance indicates the scores and where they came from
type provenance struct {
	Scores    []score `json:"scores,omitempty"`
	AgentRole string  `json:"agentRole,omitempty"`
	AtTime    string  `json:"atTime,omitempty"`
}

//Score represents one of our scores for the annotation
type score struct {
	ScoringSystem string  `json:"scoringSystem,omitempty"`
	Value         float64 `json:"value,omitempty"`
}

const (
	mentionsPred            = "http://www.ft.com/ontology/annotation/mentions"
	mentionsRel             = "MENTIONS"
	relevanceScoringSystem  = "http://api.ft.com/scoringsystem/FT-RELEVANCE-SYSTEM"
	confidenceScoringSystem = "http://api.ft.com/scoringsystem/FT-CONFIDENCE-SYSTEM"
)

var relations = map[string]string{ //TODO what predicate?
	"mentions":                "MENTIONS",
}