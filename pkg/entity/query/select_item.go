package query

import (
	"fmt"
	"github.com/goccy/go-json"
)

type SelectItem struct {
	Col   string `json:"col"`
	Alias string `json:"alias"`
	Opt   string `json:"opt"`
}

func (item *SelectItem) String() string {
	if item.Opt != "" && item.Alias != "" {
		return fmt.Sprintf("%s as %s", item.Opt, item.Alias)
	}
	if item.Alias != "" {
		return fmt.Sprintf("%s as %s", item.Col, item.Alias)
	}
	return item.Col
}

func parseSelectItems(data []byte) ([]*SelectItem, error) {
	var items []any
	err := json.Unmarshal(data, &items)
	if err != nil {
		return nil, err
	}
	var result []*SelectItem
	for _, item := range items {
		switch iVal := item.(type) {
		case string:
			result = append(result, &SelectItem{Col: iVal})
		case map[string]any:
			aItem := SelectItem{}
			aItem.Col, _ = iVal["col"].(string)
			aItem.Alias, _ = iVal["alias"].(string)
			aItem.Opt, _ = iVal["opt"].(string)
			result = append(result, &aItem)
		default:
			fmt.Printf("unknown type: %T\n", iVal)
		}
	}
	return result, nil
}
