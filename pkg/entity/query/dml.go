package query

import (
	"encoding/json"
	"fmt"
	"github.com/everpan/idig/pkg/entity/meta"
	"xorm.io/builder"
	//"github.com/goccy/go-json"
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
	tableName string // entity name or table name
	pkNum     int    // first pkNum cols is pk
	cols      []string
	vals      [][]any
}

func (cv *ColumnValue) Values() [][]any {
	return cv.vals
}

func (cv *ColumnValue) Reset() {
	cv.tableName = ""
	cv.cols = nil
	cv.vals = nil
}

func (cv *ColumnValue) DetectValueStart() int {
	if cv.vals == nil {
		return -1
	}
	if cv.pkNum < 0 {
		return -1
	}
	if cv.pkNum == 0 {
		return 0
	}
	if cv.vals[0] == nil {
		return -1
	}
	if cv.vals[0][cv.pkNum-1] == nil {
		return cv.pkNum
	}
	return -1
}

func (cv *ColumnValue) Valid() error {
	if cv.cols == nil {
		return fmt.Errorf("ColumnValue invalid column")
	}
	if cv.vals == nil {
		return fmt.Errorf("ColumnValue invalid value")
	}
	if len(cv.cols) != len(cv.vals[0]) {
		return fmt.Errorf("ColumnValue column length must be equal value length")
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
			if tmp, err1 := parseSingleValue(cv.cols, r); err1 != nil {
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
					if tmp, err1 := parseSingleValue(cv.cols, r1); err1 != nil {
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
	if err := cv.Valid(); err != nil {
		return nil, err
	}
	var ret = map[string]*ColumnValue{}
	var colIdx = map[string]int{}
	var pkIdx = -1
	for i, col := range cv.cols {
		if col == m.Entity.PkAttrField {
			pkIdx = i
		}
		if colMeta, ok := m.ColumnIndex[col]; ok {
			if cv2, ok2 := ret[colMeta.TableName]; ok2 {
				cv2.cols = append(cv2.cols, col)
			} else {
				cv2 = &ColumnValue{
					tableName: colMeta.TableName,
					pkNum:     1,
					cols:      []string{m.Entity.PkAttrField, col},
				}
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
			if pkIdx > -1 {
				dv[0] = sv[pkIdx]
			}
			for i, col := range cv3.cols[1:] {
				idx := colIdx[col]
				dv[1+i] = sv[idx]
			}
			cv3.vals = append(cv3.vals, dv)
		}
	}
	return ret, nil
}

// BuildInsertSQLWithPk 构建insert语句
func (cv *ColumnValue) BuildInsertSQLWithPk(bld *builder.Builder) {
	cv.BuildInsertSQLOffset(bld, 0)
}

// BuildInsertSQLWithoutPk 构建的语句中不包含pk的值，通常自增主键不需要
func (cv *ColumnValue) BuildInsertSQLWithoutPk(bld *builder.Builder) {
	cv.BuildInsertSQLOffset(bld, cv.pkNum)
}

func (cv *ColumnValue) BuildInsertSQLOffset(bld *builder.Builder, off int) {
	bld.Into(cv.tableName)
	var eqs []any
	if off < 0 {
		off = 0
	}
	vals := cv.vals[0][off:]
	for i, col := range cv.cols[off:] {
		eqs = append(eqs, builder.Eq{col: vals[i]})
	}
	bld.Insert(eqs...)
}

func (cv *ColumnValue) BuildUpdateSQL(bld *builder.Builder, wheres []*Where) error {
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
	bld.Update(eqs...).From(cv.tableName)
	return nil
}

func (cv *ColumnValue) CopyPkValues(pk *ColumnValue) {
	if cv.pkNum <= 0 {
		return
	}
	if cv.tableName == pk.tableName {
		return
	}
	for i, v := range cv.vals {
		for j := 0; j < pk.pkNum; i++ {
			v[j] = pk.vals[i][j]
		}
	}
}
