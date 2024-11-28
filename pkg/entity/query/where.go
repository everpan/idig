package query

import (
	"errors"
	"fmt"
	"github.com/goccy/go-json"
	"xorm.io/builder"
)

type Where struct {
	Col      string   `json:"col"`
	Op       string   `json:"op"` // operate
	Val      any      `json:"val"`
	Tie      string   `json:"tie,omitempty"`   // 与上一个where的接连方式
	SubWhere []*Where `json:"where,omitempty"` // 子条件
}

func parseWhere(data []byte) ([]*Where, error) {
	if data == nil {
		return nil, nil
	}
	var result []*Where
	err := json.Unmarshal(data, &result)
	if err != nil {
		return nil, err
	}
	err = VerifyWhere(result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (w *Where) BuildSQL(bld *builder.Builder) error {
	cond, err := w.ToCond()
	if err != nil {
		return err
	}
	if w.Tie == "or" {
		bld.Or(cond)
	} else {
		bld.And(cond)
	}
	return nil
}

func BuildWheresSQL(bld *builder.Builder, wheres []*Where) error {
	if len(wheres) > 0 {
		var err error
		for _, w := range wheres {
			err = w.BuildSQL(bld)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (w *Where) ToCond() (builder.Cond, error) {
	var cond builder.Cond
	switch w.Op {
	case "eq":
		cond = builder.Eq{w.Col: w.Val}
	case "ne":
		cond = builder.Neq{w.Col: w.Val}
	case "lt":
		cond = builder.Lt{w.Col: w.Val}
	case "lte":
		cond = builder.Lte{w.Col: w.Val}
	case "like":
		cond = builder.Like{w.Col, fmt.Sprintf("%v", w.Val)}
	case "gt":
		cond = builder.Gt{w.Col: w.Val}
	case "gte":
		cond = builder.Gte{w.Col: w.Val}
	case "in":
		cond = builder.In(w.Col, w.Val)
	case "notin":
		cond = builder.NotIn(w.Col, w.Val)
	case "expr":
		// cond = builder.Expr(w.Col, w.Val)
		return nil, fmt.Errorf("expr not impl")
	case "isnull":
		cond = builder.IsNull{w.Col}
	case "notnull":
		cond = builder.NotNull{w.Col}
	case "between":
		bv, ok := w.Val.([]any)
		if ok && len(bv) > 1 {
			cond = builder.Between{Col: w.Col, LessVal: bv[0], MoreVal: bv[1]}
		} else {
			return nil, fmt.Errorf("between vals must be arrary,and len gte two")
		}
	}
	return cond, nil
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
	for _, w := range ws {
		err = w.Verify()
		if err != nil {
			return err
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
