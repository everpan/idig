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
		Path:    "/entity/dq", // 数据查询
		Handler: queryPost,
		Method:  fiber.MethodPost,
	},
	{
		Path:    "/entity/dq/:q", // 数据查询
		Handler: paramQuery,
		Method:  fiber.MethodGet,
	},
}
var logger = core.GetLogger()

func init() {
	core.RegisterRouter(queryRoutes)
}

// paramQuery 处理 GET 请求，查询参数 q 的值
func paramQuery(ctx *core.Context) error {
	qStr := ctx.Fiber().Params("q", "")
	if err := validateQueryParam(qStr); err != nil {
		return ctx.SendBadRequestError(err)
	}

	qData, err := decodeQueryParam(qStr)
	if err != nil {
		return ctx.SendBadRequestError(err)
	}

	return queryData(ctx, qData)
}

// queryPost 处理 POST 请求，查询请求体中的数据
func queryPost(ctx *core.Context) error {
	qData := ctx.Fiber().Body()
	return queryData(ctx, qData)
}

// validateQueryParam 验证查询参数是否有效
func validateQueryParam(qStr string) error {
	if qStr == "" {
		return fmt.Errorf("q 不能为空")
	}
	return nil
}

// decodeQueryParam 解码查询参数
func decodeQueryParam(qStr string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(qStr)
}

// queryData 从查询 DSL 中通过实体查询数据
func queryData(ctx *core.Context, data []byte) error {
	tenant := ctx.Tenant()
	if tenant == nil {
		return ctx.SendBadRequestError(fmt.Errorf("未找到租户"))
	}

	q := query.NewQuery(tenant.TenantIdx, ctx.Engine())
	if err := q.Parse(data); err != nil {
		return ctx.SendBadRequestError(err)
	}

	bld := builder.Dialect(ctx.Engine().DriverName())
	if err := q.BuildSQL(bld); err != nil {
		return ctx.SendJSON(-1, "构建查询错误", err.Error())
	}

	sql, err2 := bld.ToBoundSQL()
	if err2 != nil {
		return ctx.SendJSON(-1, "构建 SQL 错误", err2.Error())
	}

	logger.Info("dq", zap.String("sql", sql))

	ret, err := ctx.Engine().QueryInterface(sql)
	if err != nil {
		return ctx.SendJSON(-1, "查询错误", err.Error())
	}

	return sendResponse(ctx, ret)
}

// sendResponse 根据请求的格式发送响应
func sendResponse(ctx *core.Context, ret []map[string]any) error {
	if ctx.Fiber().Get("X-DATA-FORMAT") == "data-table" {
		dt := &query.JDataTable{}
		dt.FromArrayMap(ret)
		return ctx.SendSuccess(dt)
	}
	return ctx.SendSuccess(ret)
}
