package meta

import (
	"encoding/json"
	"fmt"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"xorm.io/xorm"
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
	m, err := AcquireMeta("user", engine)
	assert.NoError(t, err)

	keys := m.UniqueKeys()
	assert.NotEmpty(t, keys)
}

func TestEntityMeta_HasAutoIncrement(t *testing.T) {
	m, err := AcquireMeta("user", engine)
	assert.NoError(t, err)

	assert.True(t, m.HasAutoIncrement())
}
