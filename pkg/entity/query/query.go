package query

import (
	// "encoding/json"
	"errors"
	"fmt"
	"github.com/goccy/go-json"
	"strings"
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

type BuilderSQL interface {
	BuildSQL(bld *builder.Builder) error
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

// BuildSQL 构建order/limit/where
func (q *Query) BuildSQL(bld *builder.Builder) error {
	var err error
	if len(q.Wheres) > 0 {
		for _, w := range q.Wheres {
			err = w.BuildSQL(bld)
			if err != nil {
				return err
			}
		}
	}
	if q.Orders != nil {
		var os []string
		for _, o := range q.Orders {
			os = append(os, o.String())
		}
		bld.OrderBy(strings.Join(os, ","))
	}
	if q.Limit != nil {
		bld.Limit(q.Limit.Num, q.Limit.Offset)
		// num must gt 0
	}
	return nil
}
