package handler

import (
	"database/sql"
	"fmt"
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/everpan/idig/pkg/entity/query"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"xorm.io/builder"
	"xorm.io/xorm"
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
	err2 := InsertEntityTx(engine, tableCV, m)
	if err2 != nil {
		return ctx.SendJSON(-1, "exec sql error", err2.Error())
	}
	return ctx.SendSuccess("insert ok")
}

func InsertEntityTx(engine *xorm.Engine, tableCV map[string]*query.ColumnValue, m *meta.Meta) error {
	sess := engine.NewSession()
	defer sess.Close()
	var err error
	if err = sess.Begin(); err != nil {
		return err
	}
	pkTable, ok := tableCV[m.Entity.PkAttrTable]
	if !ok {
		return fmt.Errorf("entity pk attribute not found")
	}
	dialect := engine.DriverName()
	pkRet, err := InsertColumnValue(dialect, sess, pkTable)
	if err != nil {
		return err
	}
	pkId, err := pkRet[0].LastInsertId()
	if err != nil {
		sess.Rollback()
		return err
	}
	if pkId <= 0 {
		return fmt.Errorf("entity pk is not auto increment, unsupported")
	}
	delete(tableCV, m.Entity.PkAttrTable)

	for _, cv2 := range tableCV {
		cv2.SetPk(m.Entity.PkAttrField, pkId)
		_, err2 := InsertColumnValue(dialect, sess, cv2)
		if err2 != nil {
			sess.Rollback()
			return err2
		}
	}
	return sess.Commit()
}

func InsertColumnValue(dialect string, sess *xorm.Session, cv2 *query.ColumnValue) ([]sql.Result, error) {
	bld := builder.Dialect(dialect)
	cv2.BuildInsertSQL(bld)
	sqlStr, _, err := bld.ToSQL()
	if err != nil {
		return nil, err
	}
	var rets []sql.Result
	for _, v := range cv2.Values() {
		eArgs := make([]any, 0, len(v)+1)
		eArgs = append(eArgs, sqlStr)
		eArgs = append(eArgs, v...)
		if ret, err3 := sess.Exec(eArgs...); err3 != nil {
			return nil, err3
		} else {
			rets = append(rets, ret)
		}
	}
	return rets, nil
}
