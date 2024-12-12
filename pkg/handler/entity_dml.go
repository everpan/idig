package handler

import (
	"fmt"
	"slices"

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
	{
		Path:    "/entity/dm/:entity?", // data query
		Handler: dmlUpdate,
		Method:  fiber.MethodPut,
	},
}

func init() {
	config.RegisterRouter(dmlRoutes)
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

// prepareEntityOperation 准备实体操作的通用逻辑
func prepareEntityOperation(ctx *config.Context) (*query.ColumnValue, *meta.EntityMeta, *xorm.Engine, error) {
	entityName, cv, err := parseToColumnValue(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	engine := ctx.Engine()
	m, err := meta.AcquireMeta(entityName, engine)
	if err != nil {
		return nil, nil, nil, err
	}
	return cv, m, engine, nil
}

// handleTransaction 处理事务的通用逻辑
func handleTransaction(engine *xorm.Engine, operation func(*xorm.Session) error) error {
	sess := engine.NewSession()
	defer func(sess *xorm.Session) {
		_ = sess.Close()
	}(sess)

	if err := operation(sess); err != nil {
		_ = sess.Rollback()
		return err
	}

	return sess.Commit()
}

// dmlUpdate 多值update,自动寻找 pk uk
func dmlUpdate(ctx *config.Context) error {
	cv, m, engine, err := prepareEntityOperation(ctx)
	dt := cv.DataTable()
	if err != nil {
		return ctx.SendBadRequestError(err)
	}

	pkColumn := m.PrimaryColumn()
	pkId := dt.FetchColumnIndex(pkColumn)
	if pkId < 0 {
		return ctx.SendJSON(-2, "not implement", nil)
	}

	tabCols, err := dt.DivisionColumnsToTable(m, true)
	if err != nil {
		return ctx.SendBadRequestError(err)
	}

	err = handleTransaction(engine, func(sess *xorm.Session) error {
		for t, c := range tabCols {
			if err := UpdateEntity(engine, sess, t, c, []string{pkColumn}, dt); err != nil {
				return fmt.Errorf("update entity error: %w", err)
			}
		}
		return nil
	})

	if err != nil {
		return ctx.SendJSON(-1, "update error", err.Error())
	}
	return nil
}

func dmlInsert(ctx *config.Context) error {
	cv, m, engine, err := prepareEntityOperation(ctx)
	dt := cv.DataTable()
	if err != nil {
		return ctx.SendJSON(-1, "parse column values error", err.Error())
	}

	pkColumn := m.PrimaryColumn()
	pkId := dt.FetchColumnIndex(pkColumn)
	hasAutoIncrement := m.HasAutoIncrement()

	if !hasAutoIncrement && pkId < 0 {
		return ctx.SendBadRequestError(fmt.Errorf("primary key required"))
	}

	pkId = dt.AddColumn(pkColumn)
	tableCols, err := dt.DivisionColumnsToTable(m, true)
	if err != nil {
		return ctx.SendJSON(-1, "can't division entity to attrs groups", err.Error())
	}

	pkTable := m.PrimaryTable()
	pkCols, ok := tableCols[pkTable]
	if !ok {
		return ctx.SendJSON(-1, "no values for primary table", nil)
	}

	if hasAutoIncrement {
		pkCols = slices.Clone(pkCols)
		pkCols = slices.DeleteFunc(pkCols, func(s string) bool {
			return s == pkColumn
		})
	}

	var insertCount int
	err = handleTransaction(engine, func(sess *xorm.Session) error {
		count, err := InsertEntity(engine, sess, pkTable, pkCols, dt, hasAutoIncrement, pkId)
		if err != nil {
			return fmt.Errorf("insert data error: %w", err)
		}
		insertCount = count

		delete(tableCols, pkTable)
		for table, cols := range tableCols {
			_, err = InsertEntity(engine, sess, table, cols, dt, false, 0)
			if err != nil {
				return fmt.Errorf("insert data error: %w", err)
			}
		}
		return nil
	})

	if err != nil {
		return ctx.SendJSON(-1, "insert error", err.Error())
	}
	return ctx.SendSuccess(fmt.Sprintf("insert %d rows", insertCount))
}

func UpdateEntity(engine *xorm.Engine, sess *xorm.Session, table string, cols []string, keyCols []string, dt *query.DataTable) error {
	bld := builder.Dialect(engine.DriverName())
	bld.From(table)
	var pkCond builder.Cond
	var pkVals, vals []any
	var err error
	var valIdx []int
	if pkVals, err = dt.FetchRowDataByColumns(0, keyCols); err != nil {
		return err
	}
	if valIdx, err = dt.FetchColumnsIndex(cols); err != nil {
		return err
	}
	vals, err = dt.FetchRowData(0, valIdx)
	if err != nil {
		return err
	}
	for i, col := range keyCols {
		if pkCond == nil {
			pkCond = builder.Eq{col: pkVals[i]}
		} else {
			pkCond.And(builder.Eq{col: pkVals[i]})
		}
	}
	var valCond []builder.Cond
	for i, col := range cols {
		valCond = append(valCond, builder.Eq{col: vals[i]})
	}
	bld.Update(valCond...)
	bld.Where(pkCond)
	sql, _, err := bld.ToSQL()
	if err != nil {
		return err
	}
	for i := range dt.Values() {
		args, _ := dt.FetchRowDataWithSQL(i, valIdx, sql)
		_, err2 := sess.Exec(args...)
		if err2 != nil {
			return err
		}
	}
	return nil
}

func InsertEntity(engine *xorm.Engine, sess *xorm.Session, table string, cols []string,
	dt *query.DataTable, updateAutoInc bool, pkId int) (int, error) {
	if len(cols) == 0 {
		return 0, fmt.Errorf("insert into '%v' cols is empty", table)
	}
	// xorm builder 对插入cols进行了排序，保持一致
	pkColsIndex, err := dt.SortColumnsAndFetchIndices(cols)
	if err != nil {
		return 0, err
	}
	rd, err := dt.FetchRowData(0, pkColsIndex)
	if err != nil {
		return 0, err
	}
	bld := query.BuildInsertSQL(engine.DriverName(), table, cols, rd)
	sqlStr, _, err := bld.ToSQL()
	logger.Info("InsertEntity", zap.String("sql", sqlStr), zap.Int("row count", len(dt.Values())))
	if err != nil {
		return 0, err
	}
	insertCount := 0
	for rowId := range dt.Values() {
		args, err := dt.FetchRowDataWithSQL(rowId, pkColsIndex, sqlStr)
		if err != nil {
			return 0, err
		}
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
