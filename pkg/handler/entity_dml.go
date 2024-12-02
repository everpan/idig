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
	rowCnt := pkCv.RowCount()
	for i := 0; i < rowCnt; i++ {
		bld := builder.Dialect(dialect)
		if err := pkCv.BuildInsertSQLWithoutPk(bld, i); err != nil {
			return err
		}
		if id, err := ExecInsert(bld, sess); err != nil {
			return err
		} else {
			pkCv.SetPkVal(i, id)
		}
	}
	return nil
}

func InsertEntityAttrValues(dialect string, sess *xorm.Session, cv *query.ColumnValue) error {
	rowCnt := cv.RowCount()
	for i := 0; i < rowCnt; i++ {
		bld := builder.Dialect(dialect)
		cv.BuildInsertSQLWithPk(bld, i)
		_, err := ExecInsert(bld, sess)
		if err != nil {
			return err
		}
	}
	return nil
}

func ExecInsert(bld *builder.Builder, sess *xorm.Session) (int64, error) {
	// 因builder库，处理插入的过程中对列进行了排序，带来了应用复杂，后续优化
	sqlStr, args, err := bld.ToSQL()
	if err != nil {
		return 0, err
	}
	eArgs := make([]interface{}, len(args)+1)
	eArgs = append(eArgs, sqlStr)
	eArgs = append(eArgs, args...)
	if ret, err1 := sess.Exec(eArgs...); err1 != nil {
		return 0, err1
	} else {
		return ret.LastInsertId()
	}
	return 0, nil
}
