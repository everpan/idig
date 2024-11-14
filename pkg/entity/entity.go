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
	EntityIdx   uint32 `json:"entity_idx" xorm:"pk autoincr"`
	EntityName  string `json:"entity_name" xorm:"unique"`
	Description string `json:"desc" xorm:"desc"`
	PkAttrTable string `json:"pk_attr_table"`
	PkAttrField string `json:"pk_attr_field"`
	Status      int    `json:"status"` // 1-normal 2-del,name is updated to {name-del},because is unique
}

type AttrGroup struct {
	GroupIdx    uint32 `json:"group_idx" xorm:"pk autoincr"`
	EntityIdx   uint32 `json:"-" xorm:"index"`
	AttrTable   string `json:"attr_table" xorm:"unique"` // must real table in db
	GroupName   string `json:"group_name" xorm:"index"`
	Description string `json:"desc" xorm:"desc"`
}

type Meta struct {
	Entity     *Entity                   `json:"entity"`
	AttrGroups []*AttrGroup              `json:"attr_groups"`
	AttrTables map[string]*schemas.Table `json:"attr_tables"`
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

func GetMetaFromDBAndCached(entityName string, engine *xorm.Engine) (*Meta, error) {
	e, err := queryEntityFromDB(entityName, engine)
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, fmt.Errorf("entity `%s` not found", entityName)
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
		_ = TableSchemasCache(engine)
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
	if entityId == 0 {
		return nil, fmt.Errorf("entityId is zero")
	}
	g := &AttrGroup{EntityIdx: entityId}
	var r []*AttrGroup
	err := engine.Find(&r, g)
	return r, err
}

func attachSchemaToMeta(meta *Meta, tables map[string]*schemas.Table) error {
	gs := meta.AttrGroups
	if gs == nil || len(gs) == 0 {
		return fmt.Errorf("no attr groups")
	}
	if tables == nil || len(tables) == 0 {
		return fmt.Errorf("no attr tables")
	}
	attrTable := make(map[string]*schemas.Table)
	for _, g := range gs {
		gt, ok := tables[g.AttrTable]
		if !ok {
			return fmt.Errorf("attr table '%s' for entry '%s' not found", g.AttrTable, meta.Entity.EntityName)
		}
		attrTable[g.AttrTable] = gt
	}
	meta.AttrTables = attrTable
	return nil
}

type XX struct {
}

func (meta *Meta) Verify() error {
	var errs []error
	if meta.Entity == nil {
		errs = append(errs, fmt.Errorf("entity is nil"))
	}
	if meta.AttrGroups == nil {
		errs = append(errs, fmt.Errorf("attr_groups is nil"))
	}
	if meta.AttrTables == nil {
		errs = append(errs, fmt.Errorf("attr_tables is nil"))
	}
	if meta.AttrGroups != nil && meta.AttrTables != nil {
		if len(meta.AttrGroups) == 0 {
			errs = append(errs, fmt.Errorf("attr_groups is empty"))
		}
		if len(meta.AttrTables) == 0 {
			errs = append(errs, fmt.Errorf("attr_tables is empty"))
		} else if len(meta.AttrGroups) != len(meta.AttrTables) {
			errs = append(errs, fmt.Errorf("length of attr_groups and attr_tables is not equal"))
		}
	}
	if len(errs) != 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}
