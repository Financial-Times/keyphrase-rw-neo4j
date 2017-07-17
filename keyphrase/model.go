package keyphrase

type annotations []Annotation

type Suggestion struct {
	Suggestions annotations  `json:"suggestions"`
	Uuid string		  `json:"uuid"`
}
//Annotation is the main struct used to create and return structures
type Annotation struct {
	Thing       Thing        `json:"thing,omitempty"`
	Provenances []provenance `json:"provenances,omitempty"`
}

//Thing represents a concept being linked to
type Thing struct {
	ID        string   `json:"id,omitempty"`
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

var relations = map[string]string{
	"mentions":                "MENTIONS",
}

type PopularKeyphrase struct {
	Name string `json:"keyphrase,omitempty"`
	Count int   `json:"count,omitempty"`
}

type CoOccurrence struct {
	CoOccurranceCount string `json:"cooccurrence,omitempty"`
	ConceptLabel string `json:"conceptLabel,omitempty"`
	ConceptTypes []string `json:"conceptTypes,omitempty`
	ConceptDirectType string `json:"conceptType,omitempty`
	ConceptUUID string `json:"conceptId,omitempty"`
}

type CoOccurrences struct {
	KeyphraseLabel string `json:"keyphraseLabel,omitempty"`
	KeyphraseUUID string `json:"keyphraseUuid,omitempty"`
	CoOccurrences []CoOccurrence `json:"coOccurrences,omitempty"`
}