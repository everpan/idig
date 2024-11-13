package entity

import (
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"testing"
	"xorm.io/xorm"
)

var engine *xorm.Engine

func TestMain(m *testing.M) {
	dbFile := "./entity_test.db"
	os.Remove(dbFile)
	var err error
	engine, err = xorm.NewEngine("sqlite3", dbFile)
	if err != nil {
		panic(err)
	}
	engine.ShowSQL(true)
	defer engine.Close()
	InitTable(engine)
	fmt.Println("entity TestMain is running")
	createSeedData()
	m.Run()
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

	e1 := &Entity{EntityName: "user", PkAttrTable: "user", PkAttrField: "user_idx", Status: 1}
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
	e2 := &Entity{EntityName: "user01", PkAttrTable: "user01", PkAttrField: "user_idx", Status: 0}
	engine.Insert(e2)
}

func TestSerialMeta(t *testing.T) {
	tests := []struct {
		name    string
		meta    *Meta
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
	userEntity := &Entity{1,
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
		{"entity id = 1", 1, 2, false, ""},
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
