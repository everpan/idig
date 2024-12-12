package handler

import (
	"encoding/base64"
	"fmt"
	"github.com/everpan/idig/pkg/core"

	"github.com/everpan/idig/pkg/entity/query"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"xorm.io/builder"
)

var queryRoutes = []*core.IDigRoute{
	{
		Path:    "/entity/dq", // data query
		Handler: queryPost,
		Method:  fiber.MethodPost,
	},
	{
		Path:    "/entity/dq/:q", // data query
		Handler: paramQuery,
		Method:  fiber.MethodGet,
	},
}
var logger = core.GetLogger()

func init() {
	core.RegisterRouter(queryRoutes)
}

func paramQuery(ctx *core.Context) error {
	qStr := ctx.Fiber().Params("q", "")
	if qStr == "" {
		return ctx.SendBadRequestError(fmt.Errorf("q is empty"))
	}
	qData, err := base64.StdEncoding.DecodeString(qStr)
	if err != nil {
		return ctx.SendBadRequestError(err)
	}
	return queryData(ctx, qData)
}

func queryPost(ctx *core.Context) error {
	qData := ctx.Fiber().Body()
	return queryData(ctx, qData)
}

// queryData 从query的dsl中，通过entity 查询数据
func queryData(ctx *core.Context, data []byte) error {
	tenant := ctx.Tenant()
	if tenant == nil {
		return ctx.SendBadRequestError(fmt.Errorf("tenant not found"))
	}
	q := query.NewQuery(tenant.TenantIdx, ctx.Engine())
	err := q.Parse(data)
	if err != nil {
		return ctx.SendBadRequestError(err)
	}
	bld := builder.Dialect(ctx.Engine().DriverName())
	err = q.BuildSQL(bld)
	if err != nil {
		return ctx.SendJSON(-1, "build query error", err.Error())
	}
	sql, err2 := bld.ToBoundSQL()
	if err2 != nil {
		return ctx.SendJSON(-1, "build to sql error", err2.Error())
	}
	logger.Info("dq", zap.String("sql", sql))
	ret, err := ctx.Engine().QueryInterface(sql)
	if err != nil {
		return ctx.SendJSON(-1, "query error", err.Error())
	}
	if ctx.Fiber().Get("X-Output-Fmt") == "data-table" {
		dt := &query.JDataTable{}
		dt.FromArrayMap(ret)
		return ctx.SendSuccess(dt)
	}
	return ctx.SendSuccess(ret)
}
