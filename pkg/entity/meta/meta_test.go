package meta

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"xorm.io/xorm"
	"xorm.io/xorm/schemas"
)

var (
	engine *xorm.Engine
	meta   *EntityMeta
)

func TestMain(m *testing.M) {
	dbFile := "/tmp/entity_test.db"
	os.Remove(dbFile)
	var err error
	engine, err = xorm.NewEngine("sqlite3", dbFile)
	if err != nil {
		panic(err)
	}
	engine.ShowSQL(true)

	InitEntityTable(engine)
	fmt.Println("entity TestMain is running")
	createSeedData()
	if m.Run() == 0 {
		engine.Close()
		os.Remove(dbFile)
	}
	defer engine.Close()
}

func createSeedData() {
	type User struct {
		UserIdx uint32 `xorm:"pk autoincr"`
		Name    string
	}
	engine.Sync2(new(User))
	type UserDepartment struct {
		UserIdx  uint32 `xorm:"pk"`
		DeptName string
	}
	engine.Sync2(new(UserDepartment))

	e1 := &Entity{EntityName: "user", PkAttrTable: "user", PkAttrColumn: "user_idx", Status: EntityStatusNormal}
	_, err := engine.Insert(e1)
	if err != nil {
		panic(err)
	}
	g1 := &AttrGroup{EntityIdx: e1.EntityIdx,
		AttrTable: "user", GroupName: "User base"}
	engine.Insert(g1)
	g2 := &AttrGroup{EntityIdx: e1.EntityIdx, AttrTable: "user_department"}
	engine.Insert(g2)
	// disabled entity
	e2 := &Entity{EntityName: "user01", PkAttrTable: "user01", PkAttrColumn: "user_idx", Status: EntityStatusDeleted}
	engine.Insert(e2)

	// meta
	meta = &EntityMeta{
		Entity:     e1,
		AttrGroups: []*AttrGroup{g1, g2},
	}
}

func TestSerialMeta(t *testing.T) {
	tests := []struct {
		name    string
		meta    *EntityMeta
		want    string
		wantErr bool
	}{
		{"nil meta", nil, "", true},
		{"valid meta", meta, "", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SerialMeta(tt.meta)
			if (err != nil) != tt.wantErr {
				t.Errorf("SerialMeta() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr {
				assert.NotEmpty(t, got)
			}
		})
	}
}

func TestGetMetaFromDB(t *testing.T) {
	tests := []struct {
		name            string
		entityName      string
		wantErr         bool
		metaJsonContain string
	}{
		{"not exist entity", "not-exist", true, "entity not found"},
		{"normal", "user", false, `"attr_groups":[{"group_idx":`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getMetaFromDB(tt.entityName, engine)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.metaJsonContain)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, got)
				jd, _ := json.Marshal(got)
				t.Log(string(jd))
				assert.Contains(t, string(jd), tt.metaJsonContain)
			}
		})
	}
}

func TestAcquireMeta(t *testing.T) {
	tests := []struct {
		name       string
		entityName string
		wantErr    bool
	}{
		{"empty entity name", "", true},
		{"not exist entity", "not-exist", true},
		{"normal", "user", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := AcquireMeta(tt.entityName, engine)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, got)

				// Test cache
				cached := getMetaFromCache(tt.entityName)
				assert.NotNil(t, cached)
				assert.Equal(t, got, cached)
			}
		})
	}
}

func TestEntityMeta_GetAttrGroupTablesNameFromCols(t *testing.T) {
	m, err := AcquireMeta("user", engine)
	assert.NoError(t, err)

	tests := []struct {
		name    string
		cols    []string
		want    []string
		wantErr bool
	}{
		{"empty cols", []string{}, nil, true},
		{"not exist column", []string{"not_exist"}, nil, true},
		{"normal", []string{"name"}, []string{"user"}, false},
		{"multiple tables", []string{"name", "dept_name"}, []string{"user", "user_department"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := m.GetAttrGroupTablesNameFromCols(tt.cols)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.ElementsMatch(t, tt.want, got)
			}
		})
	}
}

func TestEntityMeta_UniqueKeys(t *testing.T) {
	_, err := AcquireMeta("user", engine)
	assert.NoError(t, err)
}

func TestEntityMeta_HasAutoIncrement(t *testing.T) {
	m, err := AcquireMeta("user", engine)
	assert.NoError(t, err)

	assert.True(t, m.HasAutoIncrement())
}

func TestAcquireMeta_ErrorCases(t *testing.T) {
	tests := []struct {
		name       string
		entityName string
		wantErr    bool
	}{
		{"empty entity name", "", true},
		{"not exist entity", "not-exist", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := AcquireMeta(tt.entityName, engine)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestGetMetaFromCache(t *testing.T) {
	metaCache.entityCache.Add("user", &EntityMeta{Entity: &Entity{EntityName: "user"}})
	result := getMetaFromCache("user")
	assert.NotNil(t, result)
	assert.Equal(t, "user", result.Entity.EntityName)
}

func TestGetMetaFromDBAndCache(t *testing.T) {
	meta, err := getMetaFromDBAndCache("user", engine)
	assert.NoError(t, err)
	assert.NotNil(t, meta)
	assert.Equal(t, "user", meta.Entity.EntityName)
}

func TestGetMetaFromDB_NotFound(t *testing.T) {
	_, err := getMetaFromDB("not-exist", engine)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "entity not found")
}

func TestQueryAttrGroupFromDB(t *testing.T) {
	entity := &Entity{EntityName: "user_x"}
	id, err := engine.Insert(entity)
	// Get the latest EntityIdx
	fmt.Printf("insert entity user, %d %d %v\n\n", id, entity.EntityIdx, err)
	ok, err := engine.ID(entity.EntityIdx).Get(entity)
	assert.True(t, ok)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, entity.EntityIdx, "EntityIdx should not be zero")
	group := &AttrGroup{EntityIdx: entity.EntityIdx, AttrTable: "user_x", GroupName: "User base"}
	engine.Truncate(group)
	// engine.Insert(group)
	// Check if the group already exists
	existingGroup := &AttrGroup{}
	has, err := engine.Where("entity_idx = ? AND attr_table = ?", entity.EntityIdx, group.AttrTable).Get(existingGroup)
	assert.NoError(t, err)

	if !has {
		// Ensure that the group is inserted correctly
		_, err := engine.Insert(group)
		assert.NoError(t, err)
	}
	groups, err := queryAttrGroupFromDB(entity.EntityIdx, engine)
	assert.NoError(t, err)
	assert.NotNil(t, groups)
	assert.Len(t, groups, 1)
	assert.Equal(t, "User base", groups[0].GroupName)
}

func TestAttachSchemaToMeta(t *testing.T) {
	meta := &EntityMeta{Entity: &Entity{PkAttrTable: "user"}}
	err := attachSchemaToMeta(meta, engine)
	assert.NoError(t, err)
}

func TestRefreshTableCache(t *testing.T) {
	err := refreshTableCache(engine)
	assert.NoError(t, err)
}

func TestBuildColumnsIndex(t *testing.T) {
	meta := &EntityMeta{Entity: &Entity{PkAttrTable: "user"}, AttrTables: map[string]*schemas.Table{}}
	meta.buildColumnsIndex()
	assert.NotNil(t, meta.ColumnIndex)
}

func TestVerify(t *testing.T) {
	meta := &EntityMeta{Entity: &Entity{EntityName: "user"}, AttrGroups: []*AttrGroup{}, AttrTables: map[string]*schemas.Table{}}
	err := meta.Verify()
	assert.Error(t, err)
}

func TestAddAttrGroup(t *testing.T) {
	meta := &EntityMeta{AttrGroups: []*AttrGroup{}}
	group := &AttrGroup{AttrTable: "user"}
	meta.AddAttrGroup(group)
	assert.Len(t, meta.AttrGroups, 1)
	meta.AddAttrGroup(group)
	assert.Len(t, meta.AttrGroups, 1)
}

func TestInsertDuplicateAttrGroup(t *testing.T) {
	entity := &Entity{EntityName: "user_duplicate"}
	engine.Insert(entity)
	engine.ID(entity.EntityIdx).Get(entity)
	group := &AttrGroup{EntityIdx: entity.EntityIdx, AttrTable: "user_duplicate", GroupName: "User base"}
	_, err := engine.Insert(group)
	assert.NoError(t, err)

	// Try to insert the same group again
	_, err = engine.Insert(group)
	assert.Error(t, err)
}

func TestQueryNonExistentAttrGroup(t *testing.T) {
	entity := &Entity{EntityName: "user_nonexistent"}
	engine.Insert(entity)
	engine.ID(entity.EntityIdx).Get(entity)
	groups, err := queryAttrGroupFromDB(entity.EntityIdx+1, engine) // Non-existent ID
	assert.NoError(t, err)
	assert.Empty(t, groups)
}

func TestQueryWithInvalidEntityIdx(t *testing.T) {
	groups, err := queryAttrGroupFromDB(99999, engine) // Invalid ID
	assert.NoError(t, err)
	assert.Empty(t, groups)
}

func TestUpdateAttrGroup(t *testing.T) {
	entity := &Entity{EntityName: "user_update"}
	engine.Insert(entity)
	engine.ID(entity.EntityIdx).Get(entity)
	group := &AttrGroup{EntityIdx: entity.EntityIdx, AttrTable: "user_update", GroupName: "User base"}
	engine.Insert(group)
	group.GroupName = "User base updated"
	_, err := engine.ID(group.GroupIdx).Update(group)
	assert.NoError(t, err)

	// Verify the update
	updatedGroup := &AttrGroup{GroupIdx: group.GroupIdx}
	engine.Get(updatedGroup)
	assert.Equal(t, "User base updated", updatedGroup.GroupName)
}

func TestConcurrentInsertAndQuery(t *testing.T) {
	entity := &Entity{EntityName: "user_concurrent"}
	engine.Insert(entity)
	engine.ID(entity.EntityIdx).Get(entity)

	var wg sync.WaitGroup
	wg.Add(1)

	// Concurrent insert and query
	go func() {
		defer wg.Done()
		group := &AttrGroup{EntityIdx: entity.EntityIdx, AttrTable: "user_concurrent_insert", GroupName: "User base"}
		_, err := engine.Insert(group)
		assert.NoError(t, err)

		// Short wait to ensure data is committed
		time.Sleep(50 * time.Millisecond)

		// Query after insert
		groups, err := queryAttrGroupFromDB(entity.EntityIdx, engine)
		assert.NoError(t, err)
		assert.NotEmpty(t, groups)
	}()

	wg.Wait()
}
