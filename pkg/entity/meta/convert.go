package meta

import (
	"github.com/goccy/go-json"
	"strings"
	"xorm.io/xorm/schemas"
)

type Attr struct {
	Name      string `json:"name"`
	Type      string `json:"type"`
	Comment   string `json:"comment"`
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
	attr.Comment = strings.TrimSpace(col.Comment)
	attr.Length1 = col.SQLType.DefaultLength
	attr.Length2 = col.SQLType.DefaultLength2
	attr.AttrTable = attrTable
}

func (m *EntityMeta) ToJMeta() *JMeta {
	mj := &JMeta{
		Entity:      m.Entity.EntityName,
		EntryInfo:   m.Entity,
		GroupInfo:   m.AttrGroups,
		PrimaryKeys: m.AttrTables[m.Entity.PkAttrTable].PrimaryKeys,
	}
	for table, schema := range m.AttrTables {
		for _, col := range schema.Columns() {
			attr := &Attr{}
			attr.FromColumn(table, col)
			mj.Attrs = append(mj.Attrs, attr)
		}
	}
	return mj
}

func (m *EntityMeta) Marshal() ([]byte, error) {
	jm := m.ToJMeta()
	return json.Marshal(jm)
}
