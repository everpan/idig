package query

import (
	// "encoding/json"
	"errors"
	"fmt"
	"github.com/goccy/go-json"
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
type EntityOrSubQuery struct {
	Entity string `json:"entity"`
	Alias  string `json:"alias"`
	Query  *Query `json:"query"`
}
type From struct {
	EntityAlias []*EntityOrSubQuery `json:"from"`
}

type Query struct {
	// Version     string        `json:"version,omitempty"`
	// Entity      string        `json:"entity,omitempty"`
	Alias       string        `json:"alias,omitempty"`
	SelectItems []*SelectItem `json:"select"`
	From        *From         `json:"from"`
	Wheres      []*Where      `json:"where,omitempty"`
	Orders      []*Order      `json:"order,omitempty"`
	Limit       *Limit        `json:"limit,omitempty"`
}

func NewQuery() *Query {
	return &Query{
		// Version: "1.0",
		From: &From{},
	}
}

func Parse(data []byte) (*Query, error) {
	q := NewQuery()
	qSt := map[string]json.RawMessage{}
	var err error
	err = json.Unmarshal(data, &qSt)
	if err != nil {
		return nil, err
	}
	if _, ok := qSt["select"]; !ok {
		return nil, errors.New(fmt.Sprint("query does not contain select items"))
	}
	if _, ok := qSt["alias"]; ok {
		q.Alias = string(qSt["alias"])
	}
	var errs [5]error
	errs[0] = q.parseSelectItems(qSt["select"])
	errs[1] = q.parseFrom(qSt["from"])
	errs[2] = q.parseWhere(qSt["where"])
	errs[3] = q.parseOrder(qSt["order"])
	errs[4] = q.parseLimit(qSt["limit"])
	for _, e := range errs {
		if e != nil {
			return nil, e
		}
	}
	return q, nil
}
func (q *Query) ToSql(jsonStr string) (string, error) {
	return "", nil
}

// parseSelectItems 解析选择字段
func (q *Query) parseSelectItems(data []byte) error {
	var items []any
	err := json.Unmarshal(data, &items)
	if err != nil {
		return err
	}
	for _, item := range items {
		switch iVal := item.(type) {
		case string:
			q.SelectItems = append(q.SelectItems, &SelectItem{Col: iVal})
		//case []byte:
		//	selectItem = append(selectItem, &SelectItem{Col: string(iVal)})
		case map[string]any:
			aItem := SelectItem{}
			aItem.Col, _ = iVal["col"].(string)
			aItem.Alias, _ = iVal["alias"].(string)
			aItem.Opt, _ = iVal["opt"].(string)
			q.SelectItems = append(q.SelectItems, &aItem)
		default:
			fmt.Printf("unknown type: %T\n", iVal)
		}
	}
	return nil
}

func (q *Query) parseWhere(data []byte) error {
	if data == nil {
		return nil
	}
	err := json.Unmarshal(data, &q.Wheres)
	if err != nil {
		return err
	}
	err = VerifyWhere(q.Wheres)
	if err != nil {
		return err
	}
	return nil
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

func (q *Query) parseOrder(data []byte) error {
	if data == nil {
		return nil
	}
	err := json.Unmarshal(data, &q.Orders)
	if err != nil {
		return err
	}
	for _, o1 := range q.Orders {
		err = o1.Verify()
		if err != nil {
			return err
		}
	}
	return nil
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

func (q *Query) parseLimit(data []byte) error {
	if data == nil {
		return nil
	}
	err := json.Unmarshal(data, q.Limit)
	if err != nil {
		return err
	}
	return nil
}

func (q *Query) parseFrom(data []byte) error {
	if data == nil {
		return fmt.Errorf("'from' is empty")
	}
	var m any
	err := json.Unmarshal(data, &m)
	if err != nil {
		return err
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
				return fmt.Errorf("unknown from type: %T, need []string", s)
			}
		}
	case map[string]any:
		ea := EntityOrSubQuery{}
		q2, err2 := Parse(data)
		if err2 != nil {
			return err2
		}
		ea.Query = q2
		f.EntityAlias = append(f.EntityAlias, &ea)
	default:
		return fmt.Errorf("unknown from type: %T", v)
	}
	q.From = &f
	return nil
}
