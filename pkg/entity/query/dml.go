package query

import (
	"github.com/everpan/idig/pkg/entity/meta"
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
	EntityName string // entity name or table name
	TenantId   any
	Meta       *meta.EntityMeta
	wheres     []*Where
	data       *DataTable
}

func (cv *ColumnValue) DataTable() *DataTable {
	if cv.data != nil {
		return cv.data
	}
	return nil
}

func (cv *ColumnValue) ParseValues(data []byte) error {
	cv.data = NewDataTable()
	err := cv.data.ParseValues(data)
	if err != nil {
		return err
	}
	// 记录插入删除结果 --result缩写
	cv.data.resultIdx = cv.data.AddColumn("#result")
	return err
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
