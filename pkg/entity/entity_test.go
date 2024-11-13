package entity

import (
	"fmt"
	_ "github.com/mattn/go-sqlite3"
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
	m.Run()
}

func createSeedData() {

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
	tests := []struct {
		name       string
		entityName string
		want       *Entity
		wantErr    bool
	}{
		{"empty", "test", nil, false},
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
