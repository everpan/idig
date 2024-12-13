package meta

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/everpan/idig/pkg/core"
	"github.com/goccy/go-json"
	lru "github.com/hashicorp/golang-lru"
	"xorm.io/xorm"
	"xorm.io/xorm/schemas"
)

// Entity status constants
const (
	EntityStatusNormal  = 1
	EntityStatusDeleted = 2
)

// Cache configuration
const (
	DefaultCacheSize    = 1000 // 默认缓存大小
	DefaultCacheTTL     = 1 * time.Hour
	DefaultQueryTimeout = 30 * time.Second
)

// Common errors
var (
	ErrNilParameter   = errors.New("nil parameter provided")
	ErrTableNotFound  = errors.New("table not found")
	ErrEntityNotFound = errors.New("entity not found")
	ErrInvalidStatus  = errors.New("invalid entity status")
)

// Entity represents the basic entity information
type Entity struct {
	EntityIdx    uint32 `json:"entity_idx" xorm:"pk autoincr"`
	EntityName   string `json:"entity_name" xorm:"unique"`
	Description  string `json:"desc" xorm:"desc_str"`
	PkAttrTable  string `json:"pk_attr_table"`
	PkAttrColumn string `json:"pk_attr_column"`
	Status       int    `json:"status"` // EntityStatusNormal or EntityStatusDeleted
}

// AttrGroup represents a group of attributes for an entity
type AttrGroup struct {
	GroupIdx    uint32 `json:"group_idx" xorm:"pk autoincr"`
	EntityIdx   uint32 `json:"entity_idx" xorm:"index"`
	AttrTable   string `json:"attr_table" xorm:"unique"` // must real table in db
	GroupName   string `json:"group_name" xorm:"index"`
	Description string `json:"desc" xorm:"desc_str"`
}

// EntityMeta contains all metadata for an entity
type EntityMeta struct {
	Entity      *Entity                    `json:"entity"`
	AttrGroups  []*AttrGroup               `json:"attr_groups"`
	AttrTables  map[string]*schemas.Table  `json:"attr_tables"`
	ColumnIndex map[string]*schemas.Column `json:"-"`
	UpdatedAt   time.Time                  `json:"-"`
}

// MetaCache manages the caching of entity metadata
type MetaCache struct {
	sync.RWMutex
	entityCache *lru.Cache
	tableCache  *lru.Cache
}

var (
	metaCache *MetaCache
	once      sync.Once
)

// initCache initializes the cache with specified size
func initCache(size int) error {
	var err error
	once.Do(func() {
		entityCache, cacheErr := lru.New(size)
		if cacheErr != nil {
			err = fmt.Errorf("failed to create entity cache: %w", cacheErr)
			return
		}

		tableCache, cacheErr := lru.New(size)
		if cacheErr != nil {
			err = fmt.Errorf("failed to create table cache: %w", cacheErr)
			return
		}

		metaCache = &MetaCache{
			entityCache: entityCache,
			tableCache:  tableCache,
		}
	})
	return err
}

func (e *Entity) TableName() string {
	return "idig_entity"
}

func (a *AttrGroup) TableName() string {
	return "idig_entity_attr_group"
}

// InitEntityTable initializes the entity-related tables
func InitEntityTable(engine *xorm.Engine) error {
	if err := initCache(DefaultCacheSize); err != nil {
		return fmt.Errorf("failed to initialize cache: %w", err)
	}

	if err := engine.Sync2(new(Entity)); err != nil {
		return fmt.Errorf("failed to sync entity table: %w", err)
	}

	if err := engine.Sync2(new(AttrGroup)); err != nil {
		return fmt.Errorf("failed to sync attr group table: %w", err)
	}

	// Register basic entities
	entities := []struct {
		name, desc, table, pk string
	}{
		{"entity", "实体信息", (&Entity{}).TableName(), "entity_idx"},
		{"entity_attr_group", "实体属性组信息", (&AttrGroup{}).TableName(), "group_idx"},
		{"tenant", "租户信息", (&core.Tenant{}).TableName(), "tenant_idx"},
	}

	for _, e := range entities {
		if _, err := RegisterEntity(engine, e.name, e.desc, e.table, e.pk); err != nil {
			return fmt.Errorf("failed to register entity %s: %w", e.name, err)
		}
	}

	return nil
}

func init() {
	core.RegisterInitTableFunction(InitEntityTable)
}

// DataSourceHash generates a secure hash for the data source name
func DataSourceHash(s string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(s)))
}

// RegisterEntity registers a new entity in the system
func RegisterEntity(engine *xorm.Engine, name, desc, pkAttrTable, pkAttrColumn string) (int64, error) {
	if name == "" || pkAttrTable == "" || pkAttrColumn == "" {
		return 0, ErrNilParameter
	}

	e := &Entity{
		EntityName:   name,
		Description:  desc,
		PkAttrTable:  pkAttrTable,
		PkAttrColumn: pkAttrColumn,
		Status:       EntityStatusNormal,
	}

	return engine.Insert(e)
}

// AcquireMeta retrieves entity metadata with caching
func AcquireMeta(entity string, engine *xorm.Engine) (*EntityMeta, error) {
	if entity == "" {
		return nil, ErrNilParameter
	}

	// Try cache first
	if meta := getMetaFromCache(entity); meta != nil {
		if time.Since(meta.UpdatedAt) < DefaultCacheTTL {
			return meta, nil
		}
	}

	// Get from DB and cache
	return getMetaFromDBAndCache(entity, engine)
}

func getMetaFromCache(entityName string) *EntityMeta {
	metaCache.RLock()
	defer metaCache.RUnlock()

	if val, ok := metaCache.entityCache.Get(entityName); ok {
		return val.(*EntityMeta)
	}
	return nil
}

func getMetaFromDBAndCache(entityName string, engine *xorm.Engine) (*EntityMeta, error) {
	meta, err := getMetaFromDB(entityName, engine)
	if err != nil {
		return nil, err
	}

	metaCache.Lock()
	defer metaCache.Unlock()

	meta.UpdatedAt = time.Now()
	metaCache.entityCache.Add(entityName, meta)
	return meta, nil
}

// getMetaFromDB retrieves entity metadata from the database
func getMetaFromDB(entityName string, engine *xorm.Engine) (*EntityMeta, error) {
	e := &Entity{
		EntityName: entityName,
		Status:     EntityStatusNormal,
	}

	exists, err := engine.Get(e)
	if err != nil {
		return nil, fmt.Errorf("failed to get entity: %w", err)
	}
	if !exists {
		return nil, ErrEntityNotFound
	}

	groups, err := queryAttrGroupFromDB(e.EntityIdx, engine)
	if err != nil {
		return nil, fmt.Errorf("failed to query attr groups: %w", err)
	}

	meta := &EntityMeta{
		Entity:     e,
		AttrGroups: groups,
		AttrTables: make(map[string]*schemas.Table),
	}

	// Add primary table as virtual attr group
	meta.AddAttrGroup(&AttrGroup{
		GroupIdx:    0,
		EntityIdx:   e.EntityIdx,
		AttrTable:   e.PkAttrTable,
		Description: "auto build virtual attr group",
	})

	// Get table schemas
	if err := attachSchemaToMeta(meta, engine); err != nil {
		return nil, fmt.Errorf("failed to attach schema: %w", err)
	}

	return meta, nil
}

// queryAttrGroupFromDB retrieves attribute groups for an entity
func queryAttrGroupFromDB(entityIdx uint32, engine *xorm.Engine) ([]*AttrGroup, error) {
	var groups []*AttrGroup
	err := engine.Where("entity_idx = ?", entityIdx).Find(&groups)
	if err != nil {
		return nil, err
	}
	return groups, nil
}

// attachSchemaToMeta attaches table schemas to entity metadata
func attachSchemaToMeta(m *EntityMeta, engine *xorm.Engine) error {
	key := DataSourceHash(engine.DataSourceName())

	metaCache.RLock()
	tables, exists := metaCache.tableCache.Get(key)
	metaCache.RUnlock()

	if !exists {
		if err := refreshTableCache(engine); err != nil {
			return err
		}
		metaCache.RLock()
		tables, _ = metaCache.tableCache.Get(key)
		metaCache.RUnlock()
	}

	tableMap := tables.(map[string]*schemas.Table)
	for _, g := range m.AttrGroups {
		t, ok := tableMap[g.AttrTable]
		if !ok {
			return fmt.Errorf("%w: %s", ErrTableNotFound, g.AttrTable)
		}
		m.AttrTables[g.AttrTable] = t
	}

	m.buildColumnsIndex()
	return nil
}

// refreshTableCache refreshes the table schema cache
func refreshTableCache(engine *xorm.Engine) error {
	sc, err := engine.DBMetas()
	if err != nil {
		return fmt.Errorf("failed to get DB metas: %w", err)
	}

	tableMap := make(map[string]*schemas.Table)
	for _, s := range sc {
		tableMap[s.Name] = s
	}

	metaCache.Lock()
	defer metaCache.Unlock()

	key := DataSourceHash(engine.DataSourceName())
	metaCache.tableCache.Add(key, tableMap)
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

// GetAttrGroupTablesNameFromCols 通过列找到列所存在的属性表; 包含主表 PkAttrTable
func (m *EntityMeta) GetAttrGroupTablesNameFromCols(cols []string) ([]string, error) {
	if len(cols) == 0 {
		return nil, ErrNilParameter
	}

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

	// Always include primary table
	// tableSet[m.Entity.PkAttrTable] = struct{}{}

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
	var keys [][]string

	// Add primary key
	pkTable := m.AttrTables[m.Entity.PkAttrTable]
	if pkTable != nil && len(pkTable.PrimaryKeys) > 0 {
		keys = append(keys, pkTable.PrimaryKeys)
	}

	// Add other unique keys
	for _, table := range m.AttrTables {
		for _, index := range table.Indexes {
			if index.Type == schemas.UniqueType {
				keys = append(keys, index.Cols)
			}
		}
	}
	return keys
}

func SerialMeta(m *EntityMeta) (string, error) {
	if m == nil {
		return "", ErrNilParameter
	}
	data, err := json.Marshal(m)
	if err != nil {
		return "", fmt.Errorf("failed to marshal meta: %w", err)
	}
	return string(data), nil
}
