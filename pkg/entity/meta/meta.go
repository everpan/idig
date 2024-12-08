package meta

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
	EntityIdx    uint32 `json:"entity_idx" xorm:"pk autoincr"`
	EntityName   string `json:"entity_name" xorm:"unique"`
	Description  string `json:"desc" xorm:"desc_str"`
	PkAttrTable  string `json:"pk_attr_table"`
	PkAttrColumn string `json:"pk_attr_column"`
	Status       int    `json:"status"` // 1-normal 2-del,name is updated to {name-del},because is unique
}

type AttrGroup struct {
	GroupIdx    uint32 `json:"group_idx" xorm:"pk autoincr"`
	EntityIdx   uint32 `json:"entity_idx" xorm:"index"`
	AttrTable   string `json:"attr_table" xorm:"unique"` // must real table in db
	GroupName   string `json:"group_name" xorm:"index"`
	Description string `json:"desc" xorm:"desc_str"`
}

type EntityMeta struct {
	Entity      *Entity                    `json:"entity"`
	AttrGroups  []*AttrGroup               `json:"attr_groups"`
	AttrTables  map[string]*schemas.Table  `json:"attr_tables"`
	ColumnIndex map[string]*schemas.Column `json:"-"`
}

func (e *Entity) TableName() string {
	return "idig_entity"
}

func (a *AttrGroup) TableName() string {
	return "idig_entity_attr_group"
}

func InitEntityTable(engine *xorm.Engine) error {
	err := engine.Sync2(new(Entity))
	if err != nil {
		return err
	}
	err = engine.Sync2(new(AttrGroup))
	if err != nil {
		return err
	}
	_, _ = RegisterEntity(engine, "entity", "实体信息", (&Entity{}).TableName(), "entity_id")
	_, _ = RegisterEntity(engine, "entity_attr_group", "实体属性组信息", (&AttrGroup{}).TableName(), "group_idx")
	_, _ = RegisterEntity(engine, "tenant", "租户信息", (&config.Tenant{}).TableName(), "tenant_idx")
	return err
}

func init() {
	config.RegisterInitTableFunction(InitEntityTable)
}

var (
	mux             sync.RWMutex
	dsTableCache    = map[string]map[string]*schemas.Table{}
	entityMetaCache = map[string]*EntityMeta{}
)

func RegisterEntity(engine *xorm.Engine, name, desc, pkAttrTable, pkAttrField string) (int64, error) {
	e := &Entity{
		EntityName:   name,
		Description:  desc,
		PkAttrTable:  pkAttrTable,
		PkAttrColumn: pkAttrField,
		Status:       1,
	}
	return engine.Insert(e)
}

func AddEntityAttrGroupByName(engine *xorm.Engine, entityName string, groupName string, attrTable string) (int64, error) {
	em, err := getMetaFromDB(entityName, engine)
	if err != nil {
		return 0, err
	}
	return AddEntityAttrGroupById(engine, em.Entity.EntityIdx, groupName, attrTable)
}

func AddEntityAttrGroupById(engine *xorm.Engine, entityIdx uint32, groupName string, attrTable string) (int64, error) {
	g := &AttrGroup{
		EntityIdx:   entityIdx,
		AttrTable:   attrTable,
		GroupName:   groupName,
		Description: attrTable,
	}
	return engine.Insert(g)
}

func SerialMeta(m *EntityMeta) (string, error) {
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

func AcquireMeta(entity string, engine *xorm.Engine) (*EntityMeta, error) {
	m := getMetaFromCache(entity)
	if m != nil {
		return m, nil
	}
	var err error
	m, err = getMetaFromDBAndCached(entity, engine)
	if err != nil {
		return nil, err
	}
	return m, err
}

func getMetaFromCache(entityName string) *EntityMeta {
	mux.RLocker()
	defer mux.RLocker()
	meta, ok := entityMetaCache[entityName]
	if !ok {
		return nil
	}
	return meta
}

func getMetaFromDBAndCached(entityName string, engine *xorm.Engine) (*EntityMeta, error) {
	em, err := getMetaFromDB(entityName, engine)
	if err != nil {
		return nil, err
	}
	mux.Lock()
	defer mux.Unlock()
	entityMetaCache[entityName] = em
	return em, nil
}

func getMetaFromDB(entityName string, engine *xorm.Engine) (*EntityMeta, error) {
	e, err := queryEntityFromDB(entityName, engine)
	if err != nil {
		return nil, err
	}
	if e == nil {
		return nil, fmt.Errorf("entity '%s' not found", entityName)
	}
	a, err := queryAttrGroupFromDB(e.EntityIdx, engine)
	if err != nil {
		return nil, err
	}
	// 如果attrs中不包含主表，则添加一个虚拟的
	meta := &EntityMeta{
		Entity:     e,
		AttrGroups: a,
	}
	meta.AddAttrGroup(&AttrGroup{
		GroupIdx:    0,
		EntityIdx:   e.EntityIdx,
		AttrTable:   e.PkAttrTable,
		Description: "auto build virtual attr group",
	})
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

func attachSchemaToMeta(m *EntityMeta, tables map[string]*schemas.Table) error {
	if m.Entity == nil {
		return fmt.Errorf("EntityMeta is nil")
	}
	gs := m.AttrGroups
	if gs == nil || len(gs) == 0 {
		return fmt.Errorf("entity:'%s' has no attr groups", m.Entity.EntityName)
	}
	if tables == nil || len(tables) == 0 {
		return fmt.Errorf("entity:'%s' has no attr tables", m.Entity.EntityName)
	}
	attrTable := make(map[string]*schemas.Table)
	for _, g := range gs {
		gt, ok := tables[g.AttrTable]
		if !ok {
			return fmt.Errorf("attr table '%s' for entry '%s' not found", g.AttrTable, m.Entity.EntityName)
		}
		attrTable[g.AttrTable] = gt
	}
	m.AttrTables = attrTable
	m.buildColumnsIndex()
	return nil
}

func (m *EntityMeta) buildColumnsIndex() {
	m.ColumnIndex = make(map[string]*schemas.Column)
	for tableName, schema := range m.AttrTables {
		for _, col := range schema.Columns() {
			col.TableName = tableName
			if col.Name == m.Entity.PkAttrColumn && tableName != m.Entity.PkAttrTable {
				// 属性表外键忽略
				continue
			}
			m.ColumnIndex[col.Name] = col
		}
	}
}

func (m *EntityMeta) Verify() error {
	var errs []error
	if m.Entity == nil {
		errs = append(errs, fmt.Errorf("entity is nil"))
	}
	if m.AttrGroups == nil {
		errs = append(errs, fmt.Errorf("attr_groups is nil"))
	}
	if m.AttrTables == nil {
		errs = append(errs, fmt.Errorf("attr_tables is nil"))
	}
	if m.AttrGroups != nil && m.AttrTables != nil {
		if len(m.AttrGroups) == 0 {
			errs = append(errs, fmt.Errorf("attr_groups is empty"))
		}
		if len(m.AttrTables) == 0 {
			errs = append(errs, fmt.Errorf("attr_tables is empty"))
		} else if len(m.AttrGroups) != len(m.AttrTables) {
			errs = append(errs, fmt.Errorf("length of attr_groups and attr_tables is not equal"))
		}
	}
	if len(errs) != 0 {
		return fmt.Errorf("%v", errs)
	}
	return nil
}

// GetAttrGroupTablesNameFromCols 通过列找到列所存在的属性表; 不包含主表 PkAttrTable
func (m *EntityMeta) GetAttrGroupTablesNameFromCols(cols []string) ([]string, error) {
	var tables []string
	tableSet := make(map[string]struct{})
	if len(cols) == 1 && cols[0] == "*" {
		for _, at := range m.AttrTables {
			tableSet[at.Name] = struct{}{}
		}
	} else {
		colIndex := m.ColumnIndex
		for _, col := range cols {
			colInfo, ok := colIndex[col]
			if !ok {
				return nil, fmt.Errorf("column '%s' not exist", col)
			}
			tableSet[colInfo.TableName] = struct{}{}
		}
	}
	delete(tableSet, m.Entity.PkAttrTable)
	for t := range tableSet {
		tables = append(tables, t)
	}
	return tables, nil
}

func (m *EntityMeta) AddAttrGroup(a *AttrGroup) {
	for _, at := range m.AttrGroups {
		if a.AttrTable == at.AttrTable {
			return
		}
	}
	m.AttrGroups = append(m.AttrGroups, a)
}

func (m *EntityMeta) PrimaryTable() string {
	return m.Entity.PkAttrTable
}

func (m *EntityMeta) PrimaryColumn() string {
	return m.Entity.PkAttrColumn
}

func (m *EntityMeta) IsPrimaryTable(table string) bool {
	return m.PrimaryTable() == table
}

func (m *EntityMeta) HasAutoIncrement() bool {
	return m.AttrTables[m.Entity.PkAttrTable].AutoIncrement != ""
}

func (m *EntityMeta) UniqueKeys() [][]string {
	var uk [][]string
	pt := m.AttrTables[m.Entity.PkAttrTable]
	for _, idx := range pt.Indexes {
		if idx.Type == schemas.UniqueType {
			uk = append(uk, idx.Cols)
		}
	}
	return uk
}
