// 重构后的 entity_dml.go

package handler

import (
	"fmt"
	"github.com/everpan/idig/pkg/core"
	"slices"

	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/everpan/idig/pkg/entity/query"
	"github.com/gofiber/fiber/v2"
	"go.uber.org/zap"
	"xorm.io/builder"
	"xorm.io/xorm"
)

var dmlRoutes = []*core.IDigRoute{
	{
		Path:    "/entity/dm/:entity?", // 数据操作
		Handler: dmlInsert,
		Method:  fiber.MethodPost,
	},
	{
		Path:    "/entity/dm/:entity?", // 数据操作
		Handler: dmlUpdate,
		Method:  fiber.MethodPut,
	},
}

func init() {
	core.RegisterRouter(dmlRoutes)
}

// parseToColumnValue 解析请求体中的列值
func parseToColumnValue(ctx *core.Context) (*query.ColumnValue, error) {
	fb := ctx.Fiber()
	cv := &query.ColumnValue{}
	cv.EntityName = fb.Params("entity")
	if cv.EntityName == "" {
		return nil, fmt.Errorf("entity name required")
	}

	if err := cv.ParseValues(fb.Body()); err != nil {
		return nil, err
	}
	return cv, nil
}

// prepareEntityOperation 准备实体操作的通用逻辑
func prepareEntityOperation(ctx *core.Context) (*query.ColumnValue, *xorm.Engine, error) {
	cv, err := parseToColumnValue(ctx)
	if err != nil {
		return nil, nil, err
	}
	engine := ctx.Engine()
	cv.Meta, err = meta.AcquireMeta(cv.EntityName, engine)
	if err != nil {
		return nil, nil, err
	}
	return cv, engine, nil
}

// handleTransaction 处理事务的通用逻辑
func handleTransaction(engine *xorm.Engine, operation func(*xorm.Session) error) error {
	sess := engine.NewSession()
	defer sess.Close()

	if err := operation(sess); err != nil {
		_ = sess.Rollback()
		return err
	}

	return sess.Commit()
}

// dmlUpdate 更新实体数据
func dmlUpdate(ctx *core.Context) error {
	cv, engine, err := prepareEntityOperation(ctx)
	if err != nil {
		return ctx.SendBadRequestError(err)
	}
	dt := cv.DataTable()
	pkColumn := cv.Meta.PrimaryColumn()
	pkId := dt.FetchColumnIndex(pkColumn)
	if pkId < 0 {
		return ctx.SendJSON(-2, "there is no pk in values, not implement", nil)
	}
	tabCols, err := dt.DivisionColumnsByTable(cv.Meta, true)
	if err != nil {
		return ctx.SendBadRequestError(err)
	}

	return handleTransaction(engine, func(sess *xorm.Session) error {
		return updateEntities(sess, tabCols, pkColumn, dt)
	})
}

// updateEntities 更新多个实体
func updateEntities(sess *xorm.Session, tabCols map[string][]string, pkColumn string, dt *query.DataTable) error {
	for t, cols := range tabCols {
		if err := UpdateEntity(sess.Engine(), sess, t, cols, []string{pkColumn}, dt); err != nil {
			return fmt.Errorf("update entity error: %w", err)
		}
	}
	return nil
}

// dmlInsert 插入实体数据
func dmlInsert(ctx *core.Context) error {
	cv, engine, err := prepareEntityOperation(ctx)
	if err != nil {
		return ctx.SendJSON(-1, "parse column values error", err.Error())
	}
	dt := cv.DataTable()
	pkColumn := cv.Meta.PrimaryColumn()
	pkId := dt.FetchColumnIndex(pkColumn)
	hasAutoIncrement := cv.Meta.HasAutoIncrement()

	if !hasAutoIncrement && pkId < 0 {
		return ctx.SendBadRequestError(fmt.Errorf("primary key required"))
	}

	pkId = dt.AddColumn(pkColumn)
	tableCols, err := dt.DivisionColumnsByTable(cv.Meta, true)
	if err != nil {
		return ctx.SendJSON(-1, "can't division entity to attrs groups", err.Error())
	}

	pkTable := cv.Meta.PrimaryTable()
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

	return handleTransaction(engine, func(sess *xorm.Session) error {
		insertCount, err := insertEntities(sess, pkTable, pkCols, dt, hasAutoIncrement, pkId)
		if err != nil {
			return err
		}
		return ctx.SendSuccess(fmt.Sprintf("insert %d rows", insertCount))
	})
}

// insertEntities 插入多个实体
func insertEntities(sess *xorm.Session, table string, cols []string, dt *query.DataTable, updateAutoInc bool, pkId int) (int, error) {
	count, err := InsertEntity(sess.Engine(), sess, table, cols, dt, updateAutoInc, pkId)
	if err != nil {
		return 0, fmt.Errorf("insert data error: %w", err)
	}
	return count, nil
}

// UpdateEntity 更新实体数据
func UpdateEntity(engine *xorm.Engine, sess *xorm.Session, table string, cols []string, keyCols []string, dt *query.DataTable) error {
	bld := builder.Dialect(engine.DriverName())
	bld.From(table)

	pkCond, _, err := buildPrimaryKeyCondition(dt, keyCols)
	if err != nil {
		return err
	}

	valIdx, vals, err := fetchValues(dt, cols)
	if err != nil {
		return err
	}

	valCond := buildValueConditions(cols, vals)
	bld.Update(valCond...)
	bld.Where(pkCond)

	sql, _, err := bld.ToSQL()
	if err != nil {
		return err
	}

	return executeUpdate(sess, dt, sql, valIdx)
}

// buildPrimaryKeyCondition 构建主键条件
func buildPrimaryKeyCondition(dt *query.DataTable, keyCols []string) (builder.Cond, []any, error) {
	pkCond := builder.NewCond()
	pkVals, err := dt.FetchRowDataByColumns(0, keyCols)
	if err != nil {
		return nil, nil, err
	}

	for i, col := range keyCols {
		pkCond = pkCond.And(builder.Eq{col: pkVals[i]})
	}

	return pkCond, pkVals, nil
}

// fetchValues 获取要更新的值
func fetchValues(dt *query.DataTable, cols []string) ([]int, []any, error) {
	valIdx, err := dt.FetchColumnsIndex(cols)
	if err != nil {
		return nil, nil, err
	}

	vals, err := dt.FetchRowData(0, valIdx)
	if err != nil {
		return nil, nil, err
	}

	return valIdx, vals, nil
}

// buildValueConditions 构建值条件
func buildValueConditions(cols []string, vals []any) []builder.Cond {
	var valCond []builder.Cond
	for i, col := range cols {
		valCond = append(valCond, builder.Eq{col: vals[i]})
	}
	return valCond
}

// executeUpdate 执行更新操作
func executeUpdate(sess *xorm.Session, dt *query.DataTable, sql string, valIdx []int) error {
	for i := range dt.Values() {
		args, _ := dt.FetchRowDataWithSQL(i, valIdx, sql)
		_, err := sess.Exec(args...)
		if err != nil {
			return err
		}
	}
	return nil
}

// InsertEntity 插入实体
func InsertEntity(engine *xorm.Engine, sess *xorm.Session, table string, cols []string,
	dt *query.DataTable, updateAutoInc bool, pkId int) (int, error) {
	if len(cols) == 0 {
		return 0, fmt.Errorf("table '%v' cols is empty", table)
	}

	// 对插入列进行排序以保持一致性
	pkColsIndex, err := dt.SortColumnsAndFetchIndices(cols)
	if err != nil {
		return 0, err
	}

	// 获取第一行数据
	rd, err := dt.FetchRowData(0, pkColsIndex)
	if err != nil {
		return 0, err
	}

	// 构建 SQL 插入语句
	bld := query.BuildInsertSQL(engine.DriverName(), table, cols, rd)
	sqlStr, _, err := bld.ToSQL()
	if err != nil {
		return 0, err
	}

	logger.Info("InsertEntity", zap.String("sql", sqlStr), zap.Int("row count", len(dt.Values())))

	return executeInsert(sess, dt, sqlStr, pkColsIndex, updateAutoInc, pkId)
}

// executeInsert 执行插入操作并处理自增主键
func executeInsert(sess *xorm.Session, dt *query.DataTable, sqlStr string, pkColsIndex []int, updateAutoInc bool, pkId int) (int, error) {
	insertCount := 0
	for rowId := range dt.Values() {
		args, err := dt.FetchRowDataWithSQL(rowId, pkColsIndex, sqlStr)
		if err != nil {
			return 0, err
		}

		ret, err1 := sess.Exec(args...)
		if err1 != nil {
			return 0, err1
		}

		if updateAutoInc {
			// 更新自增主键
			lastId, err2 := ret.LastInsertId()
			if err2 != nil {
				return 0, err2
			}
			dt.UpdateData(rowId, pkId, lastId)
		}
		insertCount++
	}
	return insertCount, nil
}
