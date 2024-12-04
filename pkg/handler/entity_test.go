package handler

import (
	"bytes"
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/core"
	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

type Student0 struct {
	Idx    uint32 `xorm:"pk autoincr"`
	Name   string `xorm:"varchar(255)"`
	Mobile string `xorm:"unique varchar(255)"`
	Card   string `xorm:"varchar(255)"`
}

type Student1 struct {
	Idx     uint32 `xorm:"unique"`
	Gender  string `xorm:"int"`
	ClassId int    `xorm:"int"`
}

func TestDM(t *testing.T) {
	tenant := config.DefaultTenant
	engine, _ := config.GetEngine(tenant.Driver, tenant.DataSource)
	engine.Sync2(new(Student0), new(Student1))
	_, err := meta.RegisterEntity(engine, "student", "Stu Test", "student0", "idx")
	//meta.
	if err != nil {
		t.Logf("register entity err: %v", err)
	}
	_, err = meta.AddEntityAttrGroupByName(engine, "student", "g1", "student1")
	if err != nil {
		t.Logf("add entity err: %v", err)
	}
	smeta, err := meta.AcquireMeta("student", engine)
	assert.Nil(t, err)
	t.Logf("student %v \n", string(smeta.ToJMeta().ToJson()))
	assert.Equal(t, 2, len(smeta.AttrTables))
	app := core.CreateApp()
	tests := []struct {
		name   string
		req    string
		entity string
	}{
		{"insert new", `{"vals":{"name":"s1","card":"c1","gender":"male"}}`, "student"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/entity/dm/"+tt.entity, bytes.NewReader([]byte(tt.req)))
			resp, err := app.Test(req, -1)
			// assert.Nil(t, err)
			body, err := io.ReadAll(resp.Body)
			t.Logf("resp: %v %v", err, string(body))
		})
	}
}
