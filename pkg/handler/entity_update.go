package handler

import (
	"fmt"
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/entity/query"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"xorm.io/builder"
)

var updateRoutes = []*config.IDigRoute{
	{
		Path:    "/entity/update",
		Handler: updatePost,
		Method:  fiber.MethodPost,
	},
}

func init() {
	config.RegisterRouter(updateRoutes)
}

func updatePost(ctx *config.Context) error {
	data := ctx.Fiber().Body()
	return updateData(ctx, data)
}

// updateData 从query的dsl中，更新数据
func updateData(ctx *config.Context, data []byte) error {
	tenant := ctx.Tenant()
	if tenant == nil {
		return ctx.SendBadRequestError(fmt.Errorf("tenant not found"))
	}
	q := query.NewUpdateQuery(tenant.TenantIdx, ctx.Engine())
	err := q.Parse(data)
	if err != nil {
		return ctx.SendBadRequestError(err)
	}

	bld := builder.Dialect(ctx.Engine().DriverName())
	err = q.BuildSQL(bld)
	if err != nil {
		return ctx.SendJSON(-1, "build update sql error", err.Error())
	}
	updateSQL, err := bld.ToBoundSQL()
	if err != nil {
		return ctx.SendJSON(-1, "build update sql error", err.Error())
	}
	logger.Info("update", zap.String("sql", updateSQL))
	_, err = ctx.Engine().Exec(updateSQL)
	if err != nil {
		return ctx.SendJSON(-1, "update error", err.Error())
	}

	return ctx.SendSuccess(nil)
}
