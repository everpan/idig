package handler

import (
	"github.com/everpan/idig/pkg/config"
	"github.com/gofiber/fiber/v2"
)

var queryRoutes = []*config.IDigRoute{
	{
		Path:    "/entity/:entity",
		Handler: queryData,
		Method:  fiber.MethodGet,
	},
}

func init() {
	config.RegisterRouter(queryRoutes)
}

// queryData 从query的dsl中，通过entity 查询数据
func queryData(ctx *config.Context) error {

	return nil
}
