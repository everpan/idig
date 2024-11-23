package query

import (
	// "encoding/json"
	"errors"
	"fmt"
	"github.com/goccy/go-json"
	"xorm.io/builder"
)

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
	q.SelectItems, errs[0] = parseSelectItems(qSt["select"])
	q.From, errs[1] = parseFrom(qSt["from"])
	q.Wheres, errs[2] = parseWhere(qSt["where"])
	q.Orders, errs[3] = parseOrder(qSt["order"])
	q.Limit, errs[4] = parseLimit(qSt["limit"])
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

func (q *Query) Build(dialect string) (*builder.Builder, error) {
	b := builder.Dialect(dialect)
	// select
	var items []string
	for _, item := range q.SelectItems {
		items = append(items, item.String())
	}
	b.Select(items...)
	// from
	b.From(q.From.EntityAlias[0].Entity)
	return b, nil
}

//func (q *Query) BuildFromEntity(dialect string, entityName string, m *meta.Meta) (*builder.Builder, error) {
//	// 单个实体，验证列，构建属性组
//	cols := make([]string, 0)
//	for _, item := range q.SelectItems {
//		cols = append(cols, item.Col)
//	}
//	// build col/schema
//}
