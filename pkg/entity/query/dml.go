package query

import (
	"fmt"
	"github.com/everpan/idig/pkg/entity/meta"
	"slices"
	"xorm.io/builder"
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
	tenantId  any
	wheres    []*Where
	data      *DataTable
}

func (cv *ColumnValue) Columns() []string {
	if cv.data != nil {
		return cv.data.Columns()
	}
	return nil
}

func (cv *ColumnValue) Values() [][]any {
	if cv.data != nil {
		return cv.data.Values()
	}
	return nil
}

func (cv *ColumnValue) ParseValues(data []byte) error {
	cv.data = NewDataTable()
	err := cv.data.ParseValues(data)
	return err
}

func DivisionColumnsToTable(m *meta.Meta, cols []string) (map[string][]string, error) {
	pkIdx := slices.Index(cols, m.Entity.PkAttrField)
	var ret = map[string][]string{}
	for _, col := range cols {
		if m1, ok := m.ColumnIndex[col]; ok {
			if colDist, ok2 := ret[m1.TableName]; ok2 {
				colDist = append(colDist, col)
			} else {
				cols2 := make([]string, 0)
				if pkIdx >= 0 { // 原始列中的pk，分布到各个表
					cols2 = append(cols2, m.Entity.PkAttrField)
				}
				cols2 = append(cols2, col)
				ret[m1.TableName] = cols2
			}
		} else {
			return nil, fmt.Errorf("column '%s' not found", col)
		}
	}
	return ret, nil
}

func BuildInsertSQL(dialect, table string, cols []string, vals []any) *builder.Builder {
	bld := builder.Dialect(dialect)
	bld.Into(table)
	var eqs []any
	eqs = append(eqs, builder.Eq{})
	eq := eqs[0].(builder.Eq)
	for i, col := range cols {
		eq[col] = vals[i]
	}
	bld.Insert(eq)
	return bld
}
