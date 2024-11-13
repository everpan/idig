package entity

import (
	"crypto/md5"
	"fmt"
	"github.com/everpan/idig/pkg/config"
	"github.com/goccy/go-json"
	"sync"
	"xorm.io/xorm"
	"xorm.io/xorm/schemas"
)

type Entity struct {
	EntityIdx   uint32 `xorm:"pk autoincr"`
	EntityName  string `xorm:"unique"`
	Description string
	PkAttrTable string
	PkAttrField string
	Status      int // 1-normal 2-del,name is updated to {name-del},because is unique
}

type AttrGroup struct {
	GroupIdx    uint32 `xorm:"pk autoincr"`
	EntityIdx   uint32
	AttrTable   string `xorm:"unique"` // must real table in db
	GroupName   string `xorm:"index"`
	Description string
}

type Meta struct {
	Entity     *Entity
	AttrGroups []*AttrGroup
	AttrTables map[string]*schemas.Table
}

func (e *Entity) TableName() string {
	return "idig_entity"
}
func (a *AttrGroup) TableName() string {
	return "idig_entity_attr_group"
}

func InitTable(engine *xorm.Engine) error {
	err := engine.Sync2(new(Entity))
	if err != nil {
		return err
	}
	err = engine.Sync2(new(AttrGroup))
	if err != nil {
		return err
	}
	return err
}

func init() {
	config.RegisterInitTableFunction(InitTable)
}

var (
	mux             sync.RWMutex
	dsTableCache    = map[string]map[string]*schemas.Table{}
	entityMetaCache = map[string]*Meta{}
)

func SerialMeta(m *Meta) (string, error) {
	data, err := json.Marshal(m)
	if err != nil {
		return "", err
	}
	return string(data), nil
}
func DataSourceNameMd5(s string) string {
	return fmt.Sprintf("%x", md5.Sum([]byte(s)))
}
func TableSchemasCache(engine *xorm.Engine) error {
	sc, err := engine.DBMetas()
	if err != nil {
		return err
	}
	mux.Lock()
	defer mux.Unlock()
	tableCache := make(map[string]*schemas.Table)
	for _, s := range sc {
		tableCache[s.Name] = s
	}
	key := DataSourceNameMd5(engine.DataSourceName())
	dsTableCache[key] = tableCache
	return nil
}

func GetMetaFromCache(entityName string) *Meta {
	mux.RLocker()
	defer mux.RLocker()
	meta, ok := entityMetaCache[entityName]
	if !ok {
		return nil
	}
	return meta
}

func GetMetaFromDB(entityName string, engine *xorm.Engine) (*Meta, error) {
	e, err := queryEntityFromDB(entityName, engine)
	if err != nil {
		return nil, err
	}
	a, err := queryAttrGroupFromDB(e.EntityIdx, engine)
	if err != nil {
		return nil, err
	}
	meta := &Meta{
		Entity:     e,
		AttrGroups: a,
	}
	key := DataSourceNameMd5(engine.DataSourceName())
	tables, ok := dsTableCache[key]
	if !ok {
		TableSchemasCache(engine)
		tables = dsTableCache[key]
	}
	err = attachSchemaToMeta(meta, tables)
	if err != nil {
		return nil, err
	}
	mux.Lock()
	defer mux.Unlock()
	entityMetaCache[entityName] = meta
	return meta, nil
}

func queryEntityFromDB(entityName string, engine *xorm.Engine) (*Entity, error) {
	e := &Entity{
		EntityName: entityName,
		Status:     1,
	}
	ok, err := engine.Get(e)
	if ok {
		return e, nil
	}
	return nil, err
}

func queryAttrGroupFromDB(entityId uint32, engine *xorm.Engine) ([]*AttrGroup, error) {
	return nil, fmt.Errorf("not impl")
}

func attachSchemaToMeta(meta *Meta, tables map[string]*schemas.Table) error {
	return fmt.Errorf("not impl")
}
