package entity

import (
	"github.com/goccy/go-json"
	"strings"
	"xorm.io/xorm/schemas"
)

func (attr *Attr) fromColumn(attrTable string, col *schemas.Column) {
	attr.Name = col.Name
	attr.Type = strings.ToLower(col.SQLType.Name)
	attr.Length1 = col.SQLType.DefaultLength
	attr.Length2 = col.SQLType.DefaultLength2
	attr.AttrTable = attrTable
}

func (meta *Meta) toJMeta() *JMeta {
	mj := &JMeta{
		Entity:      meta.Entity.EntityName,
		EntryInfo:   meta.Entity,
		GroupInfo:   meta.AttrGroups,
		PrimaryKeys: meta.AttrTables[meta.Entity.PkAttrTable].PrimaryKeys,
	}
	for table, schema := range meta.AttrTables {
		for _, col := range schema.Columns() {
			attr := &Attr{}
			attr.fromColumn(table, col)
			mj.Attrs = append(mj.Attrs, attr)
		}
	}
	return mj
}

func (meta *Meta) Marshal() ([]byte, error) {
	jm := meta.toJMeta()
	return json.Marshal(jm)
}
