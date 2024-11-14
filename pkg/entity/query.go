package entity

type Where struct {
	Col      string   `json:"col"`
	Op       string   `json:"op"` // operate
	Val      string   `json:"val"`
	Mode     string   `json:"mode"`
	SubWhere []*Where `json:"sub_where,omitempty"`
}

type Order struct {
	Col    string `json:"col"`
	Option string `json:"option"`
}

type Limit struct {
	Col    string `json:"col"`
	Offset int    `json:"off"`
	Num    int    `json:"num"`
}

type Alias struct {
	Origin string `json:"col"`
	Alias  string `json:"alias"`
	Source any    `json:"source,omitempty"` // 来源
}

type ColAlias = Alias
type TableAlias = Alias

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
