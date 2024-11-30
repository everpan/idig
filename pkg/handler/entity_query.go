package handler

import (
	"encoding/base64"
	"fmt"
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/entity/query"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"xorm.io/builder"
)

var queryRoutes = []*config.IDigRoute{
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
var logger = config.GetLogger()

func init() {
	config.RegisterRouter(queryRoutes)
}

func paramQuery(ctx *config.Context) error {
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

func queryPost(ctx *config.Context) error {
	qData := ctx.Fiber().Body()
	return queryData(ctx, qData)
}

// queryData 从query的dsl中，通过entity 查询数据
func queryData(ctx *config.Context, data []byte) error {
	tenant := ctx.Tenant()
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

	return ctx.SendSuccess(ret)
}
