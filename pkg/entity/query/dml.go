package query

import (
	"fmt"
	"github.com/everpan/idig/pkg/entity/meta"
	"xorm.io/builder"

	"github.com/goccy/go-json"
)

/*
	"cols":[],
	"vals":[
		[],[]
	]
*/
/*
	"vals":{
		"col-name":"vals",
		"enable":1,
	}
*/
/*
	"vals":[{
		"col-name":"vals",
		"enable":1,
	},{
		"col-name":"vals",
		"enable":1,
	}]
*/

type ColumnValue struct { // data manager
	cols []string
	vals [][]any
}

func (cv *ColumnValue) Values() [][]any {
	return cv.vals
}
func (cv *ColumnValue) Reset() {
	cv.cols = nil
	cv.vals = nil
}

func (cv *ColumnValue) Valid() error {
	if cv.cols == nil {
		return fmt.Errorf("invalid column")
	}
	if cv.vals == nil {
		return fmt.Errorf("invalid value")
	}
	if len(cv.cols) != len(cv.vals[0]) {
		return fmt.Errorf("column length must be equal value length")
	}
	return nil
}

func (cv *ColumnValue) ParseValues(data []byte) error {
	var (
		raw map[string]any
	)
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}

	for k, v := range raw {
		fmt.Printf("key %s, raw type %T\n", k, v)
		if k == "cols" {
			for _, v1 := range v.([]any) {
				s, ok := v1.(string)
				if !ok {
					return fmt.Errorf("cols value '%v' type is '%T',need 'string' type", v1, v1)
				}
				cv.cols = append(cv.cols, s)
			}
			continue
		}
		switch r := v.(type) {
		case map[string]any:
			// single value
			cv.acquireColumnKeyFromFirstValues(r)
			tmp, err1 := parseSingleValue(cv.cols, r)
			if err1 != nil {
				return fmt.Errorf("parse single value error:%s", err1.Error())
			} else {
				cv.vals = append(cv.vals, tmp)
			}
		case []any:
			for i, a := range r {
				switch r1 := a.(type) {
				case []any:
					cv.vals = append(cv.vals, r1)
				case map[string]any:
					// multi obj vals
					if i == 0 {
						cv.acquireColumnKeyFromFirstValues(r1)
					}
					tmp, err1 := parseSingleValue(cv.cols, r1)
					if err1 != nil {
						return fmt.Errorf("parse single value error:%s", err1.Error())
					} else {
						cv.vals = append(cv.vals, tmp)
					}
				default:
					return fmt.Errorf("parse multi vals error:need array vals,not %T", r1)
				}
			}
		default:
			return fmt.Errorf("parse vals error:invalid value type: %T", r)
		}
	}
	//for _, v := range vals {
	//	vals = append(vals, v)
	//}
	return nil
}

func (cv *ColumnValue) acquireColumnKeyFromFirstValues(mv map[string]any) {
	for k := range mv {
		cv.cols = append(cv.cols, k)
	}
}

func parseSingleValue(colList []string, mv map[string]any) ([]any, error) {
	var ret []any
	for _, col := range colList {
		v, ok := mv[col]
		if !ok {
			return nil, fmt.Errorf("column '%s' not found", col)
		}
		ret = append(ret, v)
	}
	return ret, nil
}

func SubdivisionColumValueToTable(m *meta.Meta, cv *ColumnValue) (map[string]*ColumnValue, error) {
	err := cv.Valid()
	if err != nil {
		return nil, err
	}
	var ret = map[string]*ColumnValue{}
	var colIdx = map[string]int{}
	for i, col := range cv.cols {
		if colMeta, ok := m.ColumnIndex[col]; ok {
			if cv2, ok2 := ret[colMeta.TableName]; ok2 {
				cv2.cols = append(cv2.cols, col)
				// cv2.vals = append(cv2.vals)
			} else {
				cv2 = &ColumnValue{}
				cv2.cols = append(cv2.cols, col)
				ret[colMeta.TableName] = cv2
			}
			colIdx[col] = i
		} else {
			return nil, fmt.Errorf("column '%s' not found", col)
		}
	}
	// copy vals
	for _, cv3 := range ret {
		for _, sv := range cv.vals {
			dv := make([]any, len(cv3.cols))
			for i, col := range cv3.cols {
				idx := colIdx[col]
				dv[i] = sv[idx]
			}
			cv3.vals = append(cv3.vals, dv)
		}
	}
	return ret, nil
}

func (cv *ColumnValue) BuildInsertSQL(bld *builder.Builder, tName string) {
	bld.Into(tName)
	var eqs []any
	for i, col := range cv.cols {
		eqs = append(eqs, builder.Eq{col: cv.vals[0][i]})
	}
	bld.Insert(eqs...)
}

func (cv *ColumnValue) BuildUpdateSQL(bld *builder.Builder, tName string, wheres []*Where) error {
	if wheres == nil || len(wheres) == 0 {
		return fmt.Errorf("wheres is empty,can't empty when update")
	}
	err := BuildWheresSQL(bld, wheres)
	if err != nil {
		return err
	}
	var eqs []builder.Cond
	for i, col := range cv.cols {
		eqs = append(eqs, builder.Eq{col: cv.vals[0][i]})
	}
	// todo more cond, reuse where ?
	bld.Update(eqs...).From(tName)
	return nil
}
