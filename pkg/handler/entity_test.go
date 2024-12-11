package handler

import (
	"bytes"
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/core"
	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/gofiber/fiber/v2"
	"github.com/stretchr/testify/assert"
	"github.com/valyala/fasthttp"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
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

func TestDM_INSERT(t *testing.T) {
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
		check  func(string, error)
	}{
		{"insert new uk is null", `{"vals":{"name":"nam1","card":"c1","gender":"male"}}`, "student",
			func(body string, err error) {
				// sqlite unique null 的情况下 可以反复插入
				assert.Contains(t, body, "insert 1 rows")
			}},
		{"column is not exit", `{"vals":{"not-exit":"not exist","name":"nam1","card":"c1","gender":"male"}}`, "student",
			func(body string, err error) {
				assert.Contains(t, body, `"data":"column 'not-exit' not found"`)
			}},
		{"uk", `{"vals":{"mobile":"uk1","name":"nam1","card":"c1","gender":"male"}}`, "student",
			func(body string, err error) {
				var chk = strings.Contains(body, "insert 1 rows") || strings.Contains(body, "UNIQUE constraint failed")
				assert.True(t, chk)
			}},
		{"multi values", `{"vals":[{"mobile":"uk21","name":"nam1","card":"c1","gender":"male"},
{"mobile":"uk22","name":"nam1","card":"c1","gender":"male"}]}`, "student",
			func(body string, err error) {
				var chk = strings.Contains(body, "insert 2 rows") || strings.Contains(body, "UNIQUE constraint failed")
				assert.True(t, chk)
			}},
		{"array values", `{"cols":["mobile","name","card","gender","class_id"],
"vals":[["uk321","nam1","c1","male",234],["uk322","nam1","c1","male",245]]}`, "student",
			func(body string, err error) {
				var chk = strings.Contains(body, "insert 2 rows") || strings.Contains(body, "UNIQUE constraint failed")
				assert.True(t, chk)
			}},
		{"pk table empty", `{"cols":["gender","class_id"],"vals":[["uk321","nam1"],["male",245]]}`, "student",
			func(body string, err error) {
				assert.Contains(t, body, `cols is empty`)
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/api/v1/entity/dm/"+tt.entity, bytes.NewReader([]byte(tt.req)))
			resp, err := app.Test(req, -1)
			// assert.Nil(t, err)
			body, err := io.ReadAll(resp.Body)
			t.Log(tt.name, string(body))
			if tt.check != nil {
				tt.check(string(body), err)
			}
		})
	}
}

func TestDM_Update(t *testing.T) {
	tenant := config.DefaultTenant
	engine, _ := config.GetEngine(tenant.Driver, tenant.DataSource)

	// 初始化 tenant cache
	config.GetFromCache(tenant.TenantUid)
	config.ReloadTenantConfig()

	// 清理数据库
	engine.DropTables(new(Student0), new(Student1))
	engine.Sync2(new(Student0), new(Student1))

	// 清理元数据
	engine.Exec("DELETE FROM idig_entity WHERE entity_name = 'student'")
	engine.Exec("DELETE FROM idig_entity_attr_group") // 完全清理属性组表
	engine.Exec("DELETE FROM idig_entity_attr")       // 清理属性表

	_, err := meta.RegisterEntity(engine, "student", "Stu Test", "student0", "idx")
	if err != nil {
		t.Fatalf("register entity err: %v", err)
	}
	_, err = meta.AddEntityAttrGroupByName(engine, "student", "g1", "student1")
	if err != nil {
		t.Fatalf("add entity err: %v", err)
	}

	// 首先插入一些测试数据
	s0 := &Student0{
		Name:   "test1",
		Mobile: "13800138000",
		Card:   "card1",
	}
	s1 := &Student1{
		Gender:  "1",
		ClassId: 1,
	}
	_, err = engine.Insert(s0)
	if err != nil {
		t.Fatalf("insert student0 failed: %v", err)
	}
	s1.Idx = s0.Idx
	_, err = engine.Insert(s1)
	if err != nil {
		t.Fatalf("insert student1 failed: %v", err)
	}

	tests := []struct {
		name  string
		body  string
		check func(body string, err error)
	}{
		{
			name: "update single field",
			body: `{
				"entity": "student",
				"from": "student",
				"wheres": [{"col": "idx", "val": 1}],
				"vals": {
					"name": "updated_test1"
				}
			}`,
			check: func(body string, err error) {
				if err != nil {
					t.Errorf("update failed: %v", err)
				}
				// 验证更新结果
				var s Student0
				has, err := engine.Where("idx = ?", 1).Get(&s)
				if err != nil || !has {
					t.Errorf("verify update failed: %v", err)
				}
				if s.Name != "updated_test1" {
					t.Errorf("update result not match, want: updated_test1, got: %s", s.Name)
				}
			},
		},
		{
			name: "update multiple fields",
			body: `{
				"entity": "student",
				"from": "student",
				"wheres": [{"col": "idx", "val": 1}],
				"vals": {
					"name": "updated_test2",
					"mobile": "13800138001",
					"gender": "2"
				}
			}`,
			check: func(body string, err error) {
				if err != nil {
					t.Errorf("update failed: %v", err)
				}
				// 验证更新结果
				var s0 Student0
				var s1 Student1
				has, err := engine.Where("idx = ?", 1).Get(&s0)
				if err != nil || !has {
					t.Errorf("verify update student0 failed: %v", err)
				}
				has, err = engine.Where("idx = ?", 1).Get(&s1)
				if err != nil || !has {
					t.Errorf("verify update student1 failed: %v", err)
				}
				if s0.Name != "updated_test2" || s0.Mobile != "13800138001" || s1.Gender != "2" {
					t.Error("update result not match")
				}
			},
		},
		{
			name: "update with invalid where condition",
			body: `{
				"entity": "student",
				"from": "student",
				"wheres": [{"col": "idx", "val": 999}],
				"vals": {
					"name": "updated_test3"
				}
			}`,
			check: func(body string, err error) {
				if err != nil {
					t.Errorf("update failed: %v", err)
				}
				// 验证没有更新任何记录
				var s Student0
				has, err := engine.Where("name = ?", "updated_test3").Get(&s)
				if err != nil {
					t.Errorf("verify update failed: %v", err)
				}
				if has {
					t.Error("should not update any record")
				}
			},
		},
		{
			name: "update with complex where condition",
			body: `{
				"entity": "student",
				"from": "student",
				"wheres": [
					{"col": "idx", "val": 1},
					{"col": "mobile", "val": "13800138001"}
				],
				"vals": {
					"card": "updated_card"
				}
			}`,
			check: func(body string, err error) {
				if err != nil {
					t.Errorf("update failed: %v", err)
				}
				// 验证更新结果
				var s Student0
				has, err := engine.Where("idx = ?", 1).Get(&s)
				if err != nil || !has {
					t.Errorf("verify update failed: %v", err)
				}
				if s.Card != "updated_card" {
					t.Errorf("update result not match, want: updated_card, got: %s", s.Card)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := config.AcquireContext()
			defer config.ReleaseContext(ctx)
			app := fiber.New()
			fctx := app.AcquireCtx(&fasthttp.RequestCtx{})
			defer app.ReleaseCtx(fctx)

			// 设置 tenant 信息
			fctx.Request().Header.Set(config.TenantHeader, tenant.TenantUid)
			err := ctx.FromFiber(fctx) // FromFiber 会自动设置 engine
			if err != nil {
				t.Fatalf("setup context failed: %v", err)
			}

			body := []byte(tt.body)
			//err = updateData(ctx, body)
			if tt.check != nil {
				tt.check(string(body), err)
			}
		})
	}
}
