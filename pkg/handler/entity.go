package handler

import (
	"fmt"
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/entity"
	"github.com/gofiber/fiber/v2"
)

var routes = []*config.IDigRoute{
	{
		Path:    "/entity/meta/:entity",
		Handler: getMeta,
		Method:  fiber.MethodGet,
	},
}

func init() {
	config.RegisterRouter(routes)
}

func getMeta(c *config.Context) error {
	e := c.Fiber().Params("entity")
	if e == "" {
		c.SendBadRequestError(fmt.Errorf("no entity specified"))
	}
	meta := entity.GetMetaFromCache(e)
	var err error
	if meta == nil {
		meta, err = entity.GetMetaFromDBAndCached(e, c.Engine())
		if err != nil {
			c.SendBadRequestError(err)
		}
	}
	data, err := meta.Marshal()
	if err != nil {
		c.SendBadRequestError(err)
	}
	return c.SendSuccess(data)
}
