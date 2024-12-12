package entity

import (
	"github.com/everpan/idig/pkg/core"
	"github.com/everpan/idig/pkg/entity/meta"
	"sync"
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
	meta.RegisterEntity(engine, "entity_relation", "实体关系", (&Relation{}).TableName(), "relation_idx")
	return err
}

func init() {
	core.RegisterInitTableFunction(InitRelationTable)
}

var (
	muxRelation   sync.RWMutex
	relationCache = make(map[uint32]map[uint32]*Relation)
)

func queryRelation(engine *xorm.Engine, LeftEntityIdx uint32) ([]*Relation, error) {
	q := &Relation{
		EntityLeft: LeftEntityIdx,
	}
	var r []*Relation
	err := engine.Find(&r, q)
	updateRelationCache(r)
	return r, err
}

func updateRelationCache(relations []*Relation) {
	if relations == nil {
		return
	}
	muxRelation.Lock()
	defer muxRelation.Unlock()
	for _, v := range relations {
		sub, ok := relationCache[v.EntityLeft]
		if !ok {
			sub = make(map[uint32]*Relation)
			relationCache[v.EntityLeft] = sub
		}
		sub[v.EntityRight] = v
	}
}
