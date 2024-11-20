package query

import (
	"encoding/json"
	"errors"
	"fmt"
	// "github.com/goccy/go-json"
)

type SelectItem struct {
	Col   string `json:"col"`
	Alias string `json:"alias"`
	Opt   string `json:"opt"`
}

type Where struct {
	Col      string   `json:"col"`
	Op       string   `json:"op"` // operate
	Val      string   `json:"val"`
	Tie      string   `json:"tie,omitempty"`   // 与上一个where的接连方式
	SubWhere []*Where `json:"where,omitempty"` // 子条件
}

type Order struct {
	Col    string `json:"col"`
	Option string `json:"option"`
}

type Limit struct {
	Col    string `json:"col"`
	Offset int    `json:"off"`
	Num    int    `json:"num"`
}
type ObjectAlias struct {
	Entity string `json:"entity"`
	Alias  string `json:"alias"`
	Query  *Query `json:"query"`
}
type From struct {
	EntityAlias []*ObjectAlias `json:"entities"`
}

type Query struct {
	Version     string       `json:"version,omitempty"`
	SelectItems []SelectItem `json:"select"`
	From        string       `json:"from"`
	Wheres      []*Where     `json:"where,omitempty"`
	Orders      []*Order     `json:"order,omitempty"`
	Limit       *Limit       `json:"limit,omitempty"`
}
type SubQuery struct {
	Query *Query `json:"query"`
	Alias string `json:"alias"`
}

func Parse(jsonStr string) (*Query, error) {
	q := &Query{}
	qSt := map[string]json.RawMessage{}
	var err error
	err = json.Unmarshal([]byte(jsonStr), &qSt)
	if err != nil {
		return nil, err
	}
	qMsg, ok := qSt["entities"]
	if !ok {
		return nil, errors.New("'query' not found")
	}
	var entities []map[string]json.RawMessage
	err = json.Unmarshal([]byte(qMsg), &entities)
	if err != nil {
		return nil, err
	}
	if len(entities) == 0 {
		return nil, errors.New("no entities found")
	}
	//for entityName, entityData := range entities {
	//	q.parseEntityQuery(entityData)
	//}
	fmt.Printf("entity size: %d\n", len(entities))
	return q, nil
}
func (q *Query) ToSql(jsonStr string) (string, error) {
	return "", nil
}

func (q *Query) parseEntityQuery(entityData []byte) error {
	err := json.Unmarshal(entityData, &q)

	if err != nil {
		return err
	}
	return nil
}

// parseSelectItems 解析选择字段
func (q *Query) parseSelectItems(jsonStr string) ([]*SelectItem, error) {
	var items []any
	err := json.Unmarshal([]byte(jsonStr), &items)
	if err != nil {
		return nil, err
	}
	var selectItem []*SelectItem
	for _, item := range items {
		switch iVal := item.(type) {
		case string:
			selectItem = append(selectItem, &SelectItem{Col: iVal})
		//case []byte:
		//	selectItem = append(selectItem, &SelectItem{Col: string(iVal)})
		case map[string]any:
			aItem := SelectItem{}
			aItem.Col, _ = iVal["col"].(string)
			aItem.Alias, _ = iVal["alias"].(string)
			aItem.Opt, _ = iVal["opt"].(string)
			selectItem = append(selectItem, &aItem)
		default:
			fmt.Printf("unknown type: %T\n", iVal)
		}
	}
	return selectItem, nil
}

func (q *Query) parseWhere(jsonStr string) ([]*Where, error) {
	var w []*Where
	err := json.Unmarshal([]byte(jsonStr), &w)
	if err != nil {
		return nil, err
	}
	err = VerifyWhere(w)
	if err != nil {
		return nil, err
	}
	return w, nil
}

func (w *Where) Verify() error {
	if w == nil {
		return errors.New("where is nil")
	}
	if w.Tie != "" {
		if w.Tie != "and" && w.Tie != "or" {
			return errors.New("where tie must be 'and' or 'or'")
		}
	}
	if w.Col == "" {
		return errors.New("where col is required")
	}
	if w.Op == "" {
		return errors.New("where op is required")
	}
	return nil
}

func VerifyWhere(ws []*Where) error {
	if len(ws) == 0 {
		return errors.New("where is empty")
	}
	var err error
	if len(ws) == 1 {
		err = ws[0].Verify()
		if err != nil {
			return err
		}
	}
	for i, w := range ws {
		err = w.Verify()
		if err != nil {
			return err
		}
		if i > 0 {
			if w.Tie == "" {
				return errors.New("where tie is empty")
			}
		}
		if w.SubWhere != nil {
			err = VerifyWhere(w.SubWhere)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (q *Query) parseOrder(jsonStr string) ([]*Order, error) {
	var o []*Order
	err := json.Unmarshal([]byte(jsonStr), &o)
	if err != nil {
		return nil, err
	}
	for _, o1 := range o {
		err = o1.Verify()
		if err != nil {
			return nil, err
		}
	}
	return o, nil
}

func (o *Order) Verify() error {
	if o == nil {
		return errors.New("order is nil")
	}
	if o.Col == "" {
		return errors.New("order col is required")
	}
	if o.Option == "" {
		return errors.New("order option is required")
	}
	if o.Option != "desc" && o.Option != "asc" {
		return errors.New("order option must be 'desc' or 'asc'")
	}
	return nil
}

func (q *Query) parseLimit(jsonStr string) (*Limit, error) {
	var l = Limit{}
	err := json.Unmarshal([]byte(jsonStr), &l)
	if err != nil {
		return nil, err
	}
	return &l, nil
}

func (q *Query) parseFrom(jsonStr string) (*From, error) {
	var f = From{}
	var m any
	err := json.Unmarshal([]byte(jsonStr), &m)
	if err != nil {
		return nil, err
	}
	switch v := m.(type) {
	case string:
		f.EntityAlias = append(f.EntityAlias, &ObjectAlias{Entity: v})
	case []any:
		for _, s := range v {
			switch s1 := s.(type) {
			case string:
				f.EntityAlias = append(f.EntityAlias, &ObjectAlias{Entity: s1})
			case map[string]any:
				ea := ObjectAlias{}
				ea.Entity, _ = s1["entity"].(string)
				ea.Alias, _ = s1["alias"].(string)
				f.EntityAlias = append(f.EntityAlias, &ea)
			default:
				fmt.Printf("unknown 00 type: %T\n", s)
			}
		}
	case map[string]any:
		for alias, sub := range v {
			ea := ObjectAlias{}
			ea.Alias = alias
			fmt.Printf("sub %v", sub)
			subData, _ := json.Marshal(sub)
			fmt.Printf("sub data: %v", string(subData))
			q2, err2 := Parse(string(subData))
			if err2 != nil {
				return nil, err2
			}
			ea.Query = q2
		}
	default:
		return nil, fmt.Errorf("unknown type: %T", v)
	}
	return &f, nil
}
