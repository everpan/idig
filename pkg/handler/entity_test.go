package handler

import (
	"github.com/everpan/idig/pkg/core"
	"github.com/stretchr/testify/assert"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
)

func Test_getMeta(t *testing.T) {
	app := core.CreateApp()

	tests := []struct {
		name     string
		entity   string
		wantCode int
		wantStr  string
	}{
		{"fetch_not_exist", "not-exist", 400, `{"code":-1,"msg":"entity 'fetch_not_exist' not found"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/api/v1/entity/meta/"+tt.name, nil)
			resp, err := app.Test(req, -1)
			assert.NoError(t, err)
			_, err = io.ReadAll(resp.Body)
			assert.NoError(t, err)
			// t.Logf(string(body))
		})
	}
}
