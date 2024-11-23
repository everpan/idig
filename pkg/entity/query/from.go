package query

import (
	"fmt"
	"github.com/goccy/go-json"
)

type EntityOrSubQuery struct {
	Entity string `json:"entity"`
	Alias  string `json:"alias"`
	Query  *Query `json:"query"`
}
type From struct {
	EntityAlias []*EntityOrSubQuery `json:"from"`
}

func parseFrom(data []byte) (*From, error) {
	if data == nil {
		return nil, fmt.Errorf("'from' is empty")
	}
	var m any
	err := json.Unmarshal(data, &m)
	if err != nil {
		return nil, err
	}
	var f = From{}

	switch v := m.(type) {
	case string:
		f.EntityAlias = append(f.EntityAlias, &EntityOrSubQuery{Entity: v})
	case []any:
		for _, s := range v {
			switch s1 := s.(type) {
			case string:
				f.EntityAlias = append(f.EntityAlias, &EntityOrSubQuery{Entity: s1})
			case map[string]any:
				ea := EntityOrSubQuery{}
				ea.Entity, _ = s1["entity"].(string)
				ea.Alias, _ = s1["alias"].(string)
				f.EntityAlias = append(f.EntityAlias, &ea)
			default:
				// fmt.Printf("unknown 00 type: %T\n", s)
				return nil, fmt.Errorf("unknown from type: %T, need []string", s)
			}
		}
	case map[string]any:
		ea := EntityOrSubQuery{}
		q2, err2 := Parse(data)
		if err2 != nil {
			return nil, err2
		}
		ea.Query = q2
		f.EntityAlias = append(f.EntityAlias, &ea)
	default:
		return nil, fmt.Errorf("unknown from type: %T", v)
	}

	return &f, nil
}
