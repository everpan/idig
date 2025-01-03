package meta

import (
	"github.com/goccy/go-json"
	"strings"
	"xorm.io/xorm/schemas"
)

type Attr struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Length1 int64  `json:"length1,omitempty"`
	Length2 int64  `json:"length2,omitempty"`
	// AttrTable   string         `json:"attr_table"` // table name
	Nullable    bool           `json:"nullable,omitempty"`
	Default     string         `json:"default,omitempty"`
	EnumOptions map[string]int `json:"enum_options,omitempty"`
	IndexName   string         `json:"index_name,omitempty"`
	UniqueName  string         `json:"unique_name,omitempty"`
	Comment     string         `json:"comment,omitempty"`
}

type JMeta struct {
	Entity      string             `json:"entity"`
	Attrs       map[string][]*Attr `json:"attrs"`
	EntryInfo   *Entity            `json:"entry_info"`
	GroupInfo   []*AttrGroup       `json:"group_info"`
	PrimaryKeys []string           `json:"primary_keys"`
}

func (jm *JMeta) ToJson() []byte {
	jd, _ := json.Marshal(jm)
	return jd
}

func (attr *Attr) FromColumn(col *schemas.Column) {
	attr.Name = col.Name
	attr.Type = strings.ToLower(col.SQLType.Name)
	attr.Length1 = col.SQLType.DefaultLength
	attr.Length2 = col.SQLType.DefaultLength2
	// attr.AttrTable = attrTable
	attr.Nullable = col.Nullable
	attr.Default = col.Default
	attr.EnumOptions = col.EnumOptions
	attr.Comment = strings.TrimSpace(col.Comment)
	// col.Indexes
}

func (attr *Attr) ToColumn() *schemas.Column {
	st := schemas.SQLType{
		Name:           strings.ToUpper(attr.Type),
		DefaultLength:  attr.Length1,
		DefaultLength2: attr.Length2,
	}
	col := schemas.NewColumn(attr.Name, attr.Name, st, attr.Length1, attr.Length2, attr.Nullable)
	col.Comment = attr.Comment
	col.EnumOptions = attr.EnumOptions
	return col
}

func (m *EntityMeta) ToJMeta() *JMeta {
	mj := &JMeta{
		Entity:      m.Entity.EntityName,
		EntryInfo:   m.Entity,
		GroupInfo:   m.AttrGroups,
		PrimaryKeys: m.AttrTables[m.Entity.PkAttrTable].PrimaryKeys,
		Attrs:       make(map[string][]*Attr),
	}
	for table, schema := range m.AttrTables {
		for _, col := range schema.Columns() {
			attr := &Attr{}
			attr.FromColumn(col)
			mj.Attrs[table] = append(mj.Attrs[table], attr)
		}
	}
	return mj
}

func (m *EntityMeta) Marshal() ([]byte, error) {
	jm := m.ToJMeta()
	return json.Marshal(jm)
}
