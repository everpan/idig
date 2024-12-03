package handler

import (
	"fmt"
	"github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/everpan/idig/pkg/entity/query"
	"github.com/gofiber/fiber/v2"
	"slices"
	"xorm.io/xorm"
)

var dmlRoutes = []*config.IDigRoute{
	{
		Path:    "/entity/dm/:entity?", // data query
		Handler: dmlInsert,
		Method:  fiber.MethodPost,
	},
	{
		Path:    "/entity/dm/:entity?", // data query
		Handler: dmlUpdate,
		Method:  fiber.MethodPut,
	},
}

func init() {
	config.RegisterRouter(dmlRoutes)
}

func dmlUpdate(ctx *config.Context) error {
	// return dmlAction(ctx, 2)
	return nil
}

func dmlInsert(ctx *config.Context) error {
	entityName, cv, err := parseToColumnValue(ctx)
	if err != nil {
		return ctx.SendJSON(-1, "parse column values error", err.Error())
	}
	engine := ctx.Engine()
	m, err := meta.AcquireMeta(entityName, engine)
	if err != nil {
		return ctx.SendJSON(-1, "acquire meta error", err.Error())
	}
	dt := cv.DataTable()
	pkId := dt.FetchColumnIndex(m.Entity.PkAttrField)
	hasAutoIncrement := m.HasAutoIncrement()
	if !hasAutoIncrement && pkId < 0 {
		// 非自增表，无主键，不能插入
		return ctx.SendBadRequestError(fmt.Errorf("primary key required"))
	}
	dt.AddColumn(m.Entity.PkAttrField) // 增加主键，参与分组
	tableCols, err := dt.DivisionColumnsToTable(m)
	if err != nil {
		return ctx.SendJSON(-1, "can't division entity to attrs groups", err.Error())
	}
	// insert pk table
	pkTable := m.Entity.PkAttrTable
	pkCols, ok := tableCols[pkTable]
	if !ok {
		return ctx.SendJSON(-1, "no values for primary table", nil)
	}
	if hasAutoIncrement {
		// 自增表，不需要赋值主键，移除
		pkCols = slices.DeleteFunc(pkCols, func(s string) bool {
			return s == m.Entity.PkAttrField
		})
	}
	sess := engine.NewSession()
	defer func(sess *xorm.Session) {
		_ = sess.Close()
	}(sess)
	insertCount, err2 := InsertData(engine, sess, pkTable, pkCols, dt, hasAutoIncrement, pkId)
	if err2 != nil {
		return ctx.SendJSON(-1, "insert data error", err2.Error())
	}
	delete(tableCols, m.Entity.PkAttrField)
	for table, cols := range tableCols {
		_, err = InsertData(engine, sess, table, cols, dt, false, 0)
		if err != nil {
			return ctx.SendJSON(-1, "insert data error", err.Error())
		}
	}
	if err = sess.Commit(); err != nil {
		return ctx.SendJSON(-1, "commit session error", err.Error())
	}
	return ctx.SendSuccess(fmt.Sprintf("insert %d rows", insertCount))
}

func InsertData(engine *xorm.Engine, sess *xorm.Session, table string, pkCols []string,
	dt *query.DataTable, updateAutoInc bool, pkId int) (int, error) {
	// xorm builder 对插入cols进行了排序，保持一致
	pkColsIndex, err := dt.SortColumnsAndFetchIndices(pkCols)
	if err != nil {
		return 0, err
	}
	bld := query.BuildInsertSQL(engine.DriverName(), table, pkCols, dt.FetchRowData(0, pkColsIndex))
	sqlStr, _, err := bld.ToSQL()
	if err != nil {
		return 0, err
	}
	insertCount := 0
	for rowId := range dt.Values() {
		args := dt.FetchRowDataWithSQL(rowId, pkColsIndex, sqlStr)
		if ret, err1 := sess.Exec(args...); err1 != nil {
			return 0, err1
		} else {
			if updateAutoInc {
				// update auto increment pk
				lastId, err2 := ret.LastInsertId()
				if err2 != nil {
					return 0, err2
				}
				dt.UpdateData(rowId, pkId, lastId)
			}
			// else 例如uuid，已经包含pk
			insertCount++
		}
	}
	return insertCount, nil
}

func parseToColumnValue(ctx *config.Context) (string, *query.ColumnValue, error) {
	fb := ctx.Fiber()
	data := fb.Body()
	entityName := fb.Params("entity")
	if entityName == "" {
		return "", nil, fmt.Errorf("entity name required")
	}
	cv := &query.ColumnValue{}
	err := cv.ParseValues(data)
	return entityName, cv, err
}
