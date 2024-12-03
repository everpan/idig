package meta

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"testing"

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

	e1 := &Entity{EntityName: "user", PkAttrTable: "user", PkAttrColumn: "user_idx", Status: 1}
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
	e2 := &Entity{EntityName: "user01", PkAttrTable: "user01", PkAttrColumn: "user_idx", Status: 0}
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
		{"nil meta", nil, "null", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SerialMeta(tt.meta)
			if (err != nil) != tt.wantErr {
				t.Errorf("SerialMeta() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SerialMeta() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_queryEntityFromDB(t *testing.T) {
	userEntity := &Entity{4,
		"user", "", "user",
		"user_idx", 1}
	tests := []struct {
		name       string
		entityName string
		want       *Entity
		wantErr    bool
	}{
		{"empty", "test", nil, false},
		{"exist", "user",
			userEntity, false},
		{"return nil when status neq 1", "user01", nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := queryEntityFromDB(tt.entityName, engine)
			if (err != nil) != tt.wantErr {
				t.Errorf("queryEntityFromDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("queryEntityFromDB() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_queryAttrGroupFromDB(t *testing.T) {
	tests := []struct {
		name          string
		entityId      uint32
		groupSize     int
		wantErr       bool
		wantErrString string
	}{
		{"entity id must neq 0", 0, 0, true, "entityId is zero"},
		{"entity id = 1", 4, 2, false, ""},
		{"entity id = 911,not exist,return nil", 911, 0, false, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := queryAttrGroupFromDB(tt.entityId, engine)
			if (err != nil) != tt.wantErr {
				t.Errorf("queryAttrGroupFromDB() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err != nil {
				assert.Contains(t, err.Error(), tt.wantErrString)
			}
			assert.Equal(t, tt.groupSize, len(got))
			if tt.groupSize == 0 {
				assert.Nil(t, got)
			}
		})
	}
}

func Test_attachSchemaToMeta(t *testing.T) {
	metaWithNotExistGroup := &EntityMeta{
		Entity:     meta.Entity,
		AttrGroups: []*AttrGroup{{AttrTable: "not_exist_table"}},
	}
	TableSchemasCache(engine)
	tables := dsTableCache[DataSourceNameMd5(engine.DataSourceName())]
	tests := []struct {
		name          string
		meta          *EntityMeta
		tables        map[string]*schemas.Table
		wantErr       bool
		wantErrString string
	}{
		{"no attr groups", &EntityMeta{}, nil, true, "meta.Entity is nil"},
		{"no attr tables", meta, nil, true, "no attr tables"},
		{"no attr tables", meta, dsTableCache["empty"], true, "no attr tables"},
		{"normal", meta, tables, false, ""},
		{"some attr tables not found", metaWithNotExistGroup, tables, true, "attr table 'not_exist_table' for entry 'user' not found"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := attachSchemaToMeta(tt.meta, tt.tables)
			if tt.wantErr {
				assert.Contains(t, err.Error(), tt.wantErrString)
				return
			}
			assert.Equal(t, len(meta.AttrGroups), len(meta.AttrTables))
		})
	}
}

func TestGetMetaFromDB(t *testing.T) {
	tests := []struct {
		name            string
		entityName      string
		metaJsonContain string
	}{
		{"not exist entity", "not-exist", "entity 'not-exist' not found"},
		{"normal", "user", `"attr_groups":[{"group_idx":`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getMetaFromDBAndCached(tt.entityName, engine)
			if err != nil {
				assert.Contains(t, err.Error(), tt.metaJsonContain)
			} else {
				assert.NotNil(t, got)
				jd, _ := json.Marshal(got)
				t.Log(string(jd))
				assert.Contains(t, string(jd), tt.metaJsonContain)
				// 查询成功并已经缓存
				got2 := getMetaFromCache(tt.entityName)
				assert.NotNil(t, got2)
				assert.Equal(t, got, got2)
			}
		})
	}
}
