package handler

import (
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/core"
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
		EntityName:  "not-attr-entity",
		PkAttrTable: "not-attr-entity",
		PkAttrField: "not-attr-entity",
		Status:      1,
	}
	engine, _ := config.GetEngine(config.DefaultTenant.Driver, config.DefaultTenant.DataSource)
	_, _ = engine.Insert(noAttEntity)
	tests := []struct {
		name     string
		entity   string
		wantCode int
		wantStr  string
	}{
		{"fetch_not_exist", "not-exist", 400,
			`{"code":-1,"msg":"entity 'not-exist' not found"}`},
		{"not-attr-entity", "not-attr-entity", 400, "entry 'not-attr-entity' not found"},
		{"tenant", "tenant", 200, `"primary_keys":["entity_idx"]`},
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
