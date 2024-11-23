package query

import (
	"errors"
	"github.com/goccy/go-json"
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
