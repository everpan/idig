package entity

import (
	"github.com/everpan/idig/pkg/config"
	"xorm.io/xorm"
)

type Relation struct {
	RelationIdx  uint32 `xorm:"pk autoincr"`
	RelationName string `xorm:"unique"`
	Description  string `xorm:"relation_desc text"`
	EntityLeft   uint32
	EntityRight  uint32
	LeftKey      string
	RightKey     string
	RelationType string // 1:1,1:m,m:1
}

func (r *Relation) TableName() string {
	return "idig_entity_relation"
}

func InitRelationTable(engine *xorm.Engine) error {
	err := engine.Sync2(new(Relation))
	relEntity := &Entity{
		EntityName:  "entity_relation",
		PkAttrTable: (&Relation{}).TableName(),
		PkAttrField: "relation_idx",
		Status:      1,
	}
	engine.Insert(relEntity)
	return err
}

func init() {
	config.RegisterInitTableFunction(InitRelationTable)
}
