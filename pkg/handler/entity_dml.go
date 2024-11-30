package handler

import (
	"fmt"
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/everpan/idig/pkg/entity/query"
	"github.com/gofiber/fiber/v2"
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
	tableCV, err := query.SubdivisionColumValueToTable(m, cv)
	if err != nil {
		return ctx.SendJSON(-1, "can't convert column-values to table rows", err.Error())
	}
	if tableCV == nil {
		return ctx.SendBadRequestError(fmt.Errorf("request can't paser entity value"))
	}
	err2 := InsertEntityTx(engine, tableCV, m)
	if err2 != nil {
		return ctx.SendJSON(-1, "exec sql error", err2.Error())
	}
	return ctx.SendSuccess("insert ok")
}

func InsertEntityTx(engine *xorm.Engine, tableCV map[string]*query.ColumnValue, m *meta.Meta) error {
	sess := engine.NewSession()
	defer func(sess *xorm.Session) {
		_ = sess.Close()
	}(sess)
	var err error
	if err = sess.Begin(); err != nil {
		return err
	}
	pkTable, ok := tableCV[m.Entity.PkAttrTable]
	if !ok {
		return fmt.Errorf("can't find primary table for entity %s", m.Entity.PkAttrTable)
	}
	var dialect = engine.DriverName()
	if err = InsertEntityPk(dialect, sess, pkTable); err != nil {
		_ = sess.Rollback()
		return err
	}
	delete(tableCV, m.Entity.PkAttrTable)
	UpdateColumnValuePkValue(tableCV, pkTable)
	for _, cv2 := range tableCV {
		if err2 := InsertEntityAttrValues(dialect, sess, cv2); err2 != nil {
			_ = sess.Rollback()
			return err2
		}
	}
	return sess.Commit()
}

func UpdateColumnValuePkValue(tableCv map[string]*query.ColumnValue, pk *query.ColumnValue) {
	for _, cv := range tableCv {
		cv.CopyPkValues(pk)
	}
}

func InsertEntityPk(dialect string, sess *xorm.Session, pkCv *query.ColumnValue) error {
	bld := builder.Dialect(dialect)
	pkCv.BuildInsertSQLWithoutPk(bld)
	return ExecInsert(bld, pkCv, sess)
}

func InsertEntityAttrValues(dialect string, sess *xorm.Session, cv *query.ColumnValue) error {
	bld := builder.Dialect(dialect)
	cv.BuildInsertSQLWithPk(bld)
	return ExecInsert(bld, cv, sess)
}

func ExecInsert(bld *builder.Builder, cv *query.ColumnValue, sess *xorm.Session) error {
	sqlStr, _, err := bld.ToSQL()
	if err != nil {
		return err
	}
	for _, v := range cv.Values() {
		eArgs := make([]any, 0, len(v)+1)
		eArgs = append(eArgs, sqlStr)
		eArgs = append(eArgs, v...)
		if _, err1 := sess.Exec(eArgs...); err1 != nil {
			return err1
		}
	}
	return nil
}
