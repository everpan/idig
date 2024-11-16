package config

import (
	"github.com/gofiber/fiber/v2"
)

type IDigResp struct {
	Code    int    `json:"code"`
	Message string `json:"msg"`
	Data    any    `json:"data,omitempty"`
}

func NewIDigResp(code int, msg string, data any) *IDigResp {
	return &IDigResp{
		code, msg, data,
	}
}

func (c *Context) SendJSON(code int, msg string, data any) error {
	c.fb.Set(fiber.HeaderContentType, fiber.MIMEApplicationJSONCharsetUTF8)
	resp := NewIDigResp(code, msg, data)
	return c.fb.JSON(resp)
}

func (c *Context) SendBadRequestError(err error) error {
	c.fb.Status(fiber.StatusBadRequest)
	return c.SendJSON(-1, err.Error(), nil)
}

func (c *Context) SendSuccess(data any) error {
	c.fb.Status(fiber.StatusOK)
	return c.SendJSON(0, "ok", data)
}
