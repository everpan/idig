package query

import "github.com/goccy/go-json"

type Limit struct {
	Col    string `json:"col"`
	Offset int    `json:"off"`
	Num    int    `json:"num"`
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
