package meta

import (
	"context"
	"xorm.io/xorm"
	"xorm.io/xorm/schemas"
)

/**
基本数据结构,与`Attr` 保持一致
{
	"name": "entity_idx",
	"type": "unsigned int",
	"comment": "",
	"attr_table": "idig_entity",
	"nullable": false
}
*/

// Table 定义表定义
type Table struct {
	Name    string
	Comment string
	Columns []*Attr
	Charset string
}

func (t *Table) AddColumn(attr *Attr) {
	t.Columns = append(t.Columns, attr)
}

func (t *Table) CreateSchemaTable() *schemas.Table {
	table := schemas.NewEmptyTable()
	table.Name = t.Name
	table.Comment = t.Comment
	for _, attr := range t.Columns {
		col := attr.ToColumn()
		table.AddColumn(col)
	}
	if len(t.Charset) > 0 {
		table.Charset = t.Charset
	} else {
		table.Charset = "utf8mb4"
	}
	return table
}

func GenerateTableSQL(eg *xorm.Engine, table *schemas.Table) (string, error) {
	sql, b, err := eg.Dialect().CreateTableSQL(context.Background(), eg.DB(), table, "")
	if !b || err != nil {
		return "", err
	}
	return sql, nil
}

func (t *Table) CreateTable(engine *xorm.Engine) error {
	ts := t.CreateSchemaTable()
	sql, err := GenerateTableSQL(engine, ts)
	if err != nil {
		return err
	}
	_, err = engine.Exec(sql)
	return err
}
