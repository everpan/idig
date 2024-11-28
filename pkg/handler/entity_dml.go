package handler

import (
	"fmt"
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/everpan/idig/pkg/entity/query"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"xorm.io/builder"
)

var dmlRoutes = []*config.IDigRoute{
	{
		Path:    "/entity/dm/:entity?", // data query
		Handler: dmlInsert,
		Method:  fiber.MethodPost,
	},
}

func init() {
	config.RegisterRouter(dmlRoutes)
}

func dmlInsert(ctx *config.Context) error {
	fb := ctx.Fiber()
	data := fb.Body()
	entityName := fb.Params("entity")
	if entityName == "" {
		return ctx.SendBadRequestError(fmt.Errorf("entity name required"))
	}
	cv := &query.ColumnValue{}
	// err := json.Unmarshal(data, cv)
	err := cv.ParseValues(data)
	if err != nil {
		return ctx.SendBadRequestError(err)
	}
	engine := ctx.Engine()
	m, err := meta.AcquireMeta(entityName, engine)
	if err != nil {
		return ctx.SendJSON(-1, "can't acquire meta", err.Error())
	}
	logger.Info("parse column value", zap.Any("cv", cv))
	tableCV, err := query.SubdivisionColumValueToTable(m, cv)
	if err != nil {
		return ctx.SendJSON(-1, "can't convert column-values to table rows", err.Error())
	}
	if tableCV == nil {
		return ctx.SendBadRequestError(fmt.Errorf("request can't paser entity value"))
	}
	logger.Info("entity add", zap.Any("entity", entityName), zap.Any("cv", cv))
	for tName, cv2 := range tableCV {
		bld := builder.Dialect(ctx.Engine().DriverName())
		cv2.BuildInsertSQL(bld, tName)
		sql, _, err2 := bld.ToSQL()
		if err2 != nil {
			return ctx.SendJSON(-1, "build gen insert sql", err2.Error())
		}
		sess := engine.NewSession()
		defer sess.Close()
		if err = sess.Begin(); err != nil {
			return ctx.SendJSON(-1, "build gen insert sql", err.Error())
		}
		for _, v := range cv2.Values() {
			eArgs := make([]any, 0)
			eArgs = append(eArgs, sql)
			eArgs = append(eArgs, v...)
			if _, err = sess.Exec(eArgs...); err != nil {
				return ctx.SendJSON(-1, "exec insert sql", err.Error())
			}
		}
		if err = sess.Commit(); err != nil {
			return ctx.SendJSON(-1, "commit insert sql", err.Error())
		}
	}
	return ctx.SendSuccess("insert ok")
}
