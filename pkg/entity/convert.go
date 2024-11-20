package entity

import (
	"github.com/goccy/go-json"
	"strings"
	"xorm.io/xorm/schemas"
)

type Attr struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Length1   int64  `json:"length1,omitempty"`
	Length2   int64  `json:"length2,omitempty"`
	AttrTable string `json:"attr_table"` // table name
}

type JMeta struct {
	Entity      string       `json:"entity"`
	Attrs       []*Attr      `json:"attrs"`
	EntryInfo   *Entity      `json:"entry_info"`
	GroupInfo   []*AttrGroup `json:"group_info"`
	PrimaryKeys []string     `json:"primary_keys"`
}

func (attr *Attr) FromColumn(attrTable string, col *schemas.Column) {
	attr.Name = col.Name
	attr.Type = strings.ToLower(col.SQLType.Name)
	attr.Length1 = col.SQLType.DefaultLength
	attr.Length2 = col.SQLType.DefaultLength2
	attr.AttrTable = attrTable
}

func (meta *Meta) ToJMeta() *JMeta {
	mj := &JMeta{
		Entity:      meta.Entity.EntityName,
		EntryInfo:   meta.Entity,
		GroupInfo:   meta.AttrGroups,
		PrimaryKeys: meta.AttrTables[meta.Entity.PkAttrTable].PrimaryKeys,
	}
	for table, schema := range meta.AttrTables {
		for _, col := range schema.Columns() {
			attr := &Attr{}
			attr.FromColumn(table, col)
			mj.Attrs = append(mj.Attrs, attr)
		}
	}
	return mj
}

func (meta *Meta) Marshal() ([]byte, error) {
	jm := meta.ToJMeta()
	return json.Marshal(jm)
}
