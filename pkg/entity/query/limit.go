package query

import "github.com/goccy/go-json"

type Limit struct {
	Offset int `json:"offset"`
	Num    int `json:"num"`
}

func parseLimit(data []byte) (*Limit, error) {
	if data == nil {
		return nil, nil
	}
	limit := &Limit{}
	err := json.Unmarshal(data, limit)
	if err != nil {
		return nil, err
	}
	return limit, nil
}
