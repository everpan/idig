package handler

import (
	"github.com/everpan/idig/pkg/core"
	_ "github.com/everpan/idig/pkg/entity"
	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func Test_getMeta(t *testing.T) {
	app := core.CreateApp()
	noAttEntity := &meta.Entity{
		EntityName:   "not-attr-entity",
		PkAttrTable:  "not-attr-entity",
		PkAttrColumn: "not-attr-entity",
		Status:       1,
	}
	engine, _ := core.GetEngine(core.DefaultTenant.Driver, core.DefaultTenant.DataSource)
	_, _ = engine.Insert(noAttEntity)
	tests := []struct {
		name     string
		entity   string
		wantCode int
		wantStr  string
	}{
		{"fetch_not_exist", "not-exist", 400,
			`{"code":-99,"msg":"entity 'not-exist' not found"}`},
		{"not-attr-entity", "not-attr-entity", 400, "entry 'not-attr-entity' not found"},
		{"tenant", "tenant", 200, `"primary_keys":["tenant_idx"]`},
		{"entity_relation", "entity_relation", 200, `"primary_keys":["relation_idx"]`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/meta/"+tt.entity, nil)
			resp, err := app.Test(req, -1)
			assert.NoError(t, err)
			body, err := io.ReadAll(resp.Body)
			assert.NoError(t, err)
			assert.Equal(t, tt.wantCode, resp.StatusCode)
			assert.Contains(t, string(body), tt.wantStr)
			t.Log(string(body))
		})
	}
}
