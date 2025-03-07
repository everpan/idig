// 重构后的 entity_dml.go

package handler

import (
	"fmt"
	"github.com/everpan/idig/pkg/core"
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
func prepareEntityOperation(ctx *core.Context) (*query.ColumnValue, error) {
	cv, err := parseToColumnValue(ctx)
	if err != nil {
		return nil, err
	}
	engine := ctx.Engine()
	cv.Meta, err = meta.AcquireMeta(cv.EntityName, engine)
	if err != nil {
		return nil, err
	}
	return cv, nil
}

// handleTransaction 处理事务的通用逻辑
func handleTransaction(ctx *core.Context, operation func(*xorm.Session) error) error {
	sess := ctx.Engine().NewSession()
	defer func(sess *xorm.Session) {
		_ = sess.Close()
	}(sess)

	if err := sess.Begin(); err != nil {
		logger.Error("failed to begin transaction", zap.Error(err))
		// return err
	}
	if err := operation(sess); err != nil {
		_ = sess.Rollback()
		logger.Info("Rollback failed", zap.Error(err))
		return err
	}
	return sess.Commit()
}

// dmlUpdate 更新实体数据
func dmlUpdate(ctx *core.Context) error {
	cv, err := prepareEntityOperation(ctx)
	if err != nil {
		return ctx.SendBadRequestError(err)
	}
	dt := cv.DataTable()
	pkColumns := cv.Meta.PrimaryColumn()
	pkId, err := dt.FetchColumnsIndex(pkColumns, nil)
	if err != nil {
		return ctx.SendBadRequestError(err)
	}
	if len(pkId) == 0 {
		return ctx.SendJSON(-2, "there is no pk in values, not implement", nil)
	}
	tabColsKV, err := dt.DivisionColumnsKeyVal(cv.Meta)
	if err != nil {
		return ctx.SendBadRequestError(err)
	}
	if err = handleTransaction(ctx, func(sess *xorm.Session) error {
		return updateEntities(sess, tabColsKV, dt)
	}); err != nil {
		return ctx.SendBadRequestError(err)
	}
	return ctx.SendJSON(0, "update finished", nil)
}

// updateEntities 更新多个实体
func updateEntities(sess *xorm.Session, tabColsKV map[string]*query.ColumnKeyVal, dt *query.DataTable) error {
	for t, ckv := range tabColsKV {
		if err := UpdateEntity(sess, t, ckv, dt); err != nil {
			return fmt.Errorf("update entity error: %w", err)
		}
	}
	return nil
}

// dmlInsert 插入实体数据
func dmlInsert(ctx *core.Context) error {
	cv, err := prepareEntityOperation(ctx)
	if err != nil {
		return ctx.SendJSON(-1, fmt.Sprintf("Error parsing column values: %v", err), nil)
	}

	dt := cv.DataTable()
	tableColsKV, err := dt.DivisionColumnsKeyVal(cv.Meta)
	if err != nil {
		return ctx.SendJSON(-1, fmt.Sprintf("Cannot divide entity into attribute groups: %v", err), nil)
	}

	pkTable := cv.Meta.PrimaryTable()
	pkColsKV, ok := tableColsKV[pkTable]
	if !ok {
		return ctx.SendJSON(-1, "No values provided for the primary table", nil)
	}

	pkValueIsNull, _, err := dt.FirstRowColumnsIsNull(pkColsKV.KCols)
	if err != nil {
		return ctx.SendJSON(-1, fmt.Sprintf("No values for the primary table: %v", err), nil)
	}

	hasAutoIncrement := cv.Meta.HasAutoIncrement()
	if !hasAutoIncrement && pkValueIsNull {
		return ctx.SendJSON(-1, "Primary key cannot be null for non-auto increment table", nil)
	}

	if err = handleTransaction(ctx, func(sess *xorm.Session) error {
		// 插入主表，便于产生 auto increment key
		if err1 := insertEntity(sess, pkTable, pkColsKV, dt, hasAutoIncrement); err1 != nil {
			return fmt.Errorf("error inserting entity into the primary table: %w", err1)
		}

		delete(tableColsKV, pkTable)
		for t, ckv := range tableColsKV {
			if err1 := insertEntity(sess, t, ckv, dt, false); err1 != nil {
				return fmt.Errorf("error inserting entity into attribute table: %w", err1)
			}
		}
		rowCnt := len(dt.Values())
		msg := fmt.Sprintf("insert %d row(s) for entity %s", rowCnt, cv.EntityName)
		// Insertion successful, return primary key and unique key values
		var rdt any
		if hasAutoIncrement {
			ukCols := cv.Meta.FilterOutPrimaryTableUniqueCols(pkColsKV.VCols)
			ukIdx, _ := dt.FetchColumnsIndex(ukCols, nil)
			ret, _ := dt.FetchRows(ukIdx)
			rdt = &query.JDataTable{
				Cols: pkColsKV.KCols,
				Data: ret,
			}
		}
		return ctx.SendJSON(0, msg, rdt)
	}); err != nil {
		return ctx.SendJSON(-1, fmt.Sprintf("inserting entity error: %v", err), nil)
	}
	return nil
}

// insertEntities 插入实体
func insertEntity(sess *xorm.Session, table string, ckv *query.ColumnKeyVal,
	dt *query.DataTable, hasAutoIncrement bool) error {
	var (
		valIdx []int
		err    error
		cols   []string
		pkPos  = 0
	)
	if hasAutoIncrement {
		cols = ckv.VCols
		pkPos = dt.FetchColumnIndex(ckv.KCols[0])
	} else { // insert with pk value
		cols = ckv.ACols
	}
	valIdx, err = dt.FetchColumnsIndex(cols, nil)

	vals, err := dt.FetchRow(0, valIdx, nil)
	if err != nil {
		return err
	}
	bld := query.BuildInsertSQL(sess.Engine().DriverName(), table, cols, vals)
	// builder sorted cols

	sqlStr, _, err := bld.ToSQL()
	if err != nil {
		return err
	}
	logger.Info("insert entity", zap.Bool("hasAutoIncrement", hasAutoIncrement),
		zap.Any("kCols", ckv.KCols), zap.Any("vCols", ckv.VCols),
		zap.String("sql", sqlStr), zap.Int("vals size", len(dt.Values())))
	for rowId := range dt.Values() {
		if args, err2 := dt.FetchRowDataWithSQL(rowId, valIdx, nil, sqlStr); err2 != nil {
			return err2
		} else {
			insertRet, err3 := sess.Exec(args...)
			if err3 != nil {
				return err3
			}
			if hasAutoIncrement { // 执行插入操作并处理自增主键
				lastId, _ := insertRet.LastInsertId()
				_ = dt.UpdateData(rowId, pkPos, lastId)
				_ = dt.UpdateResult(rowId, lastId)
			} else {
				af, _ := insertRet.RowsAffected()
				_ = dt.UpdateAffectedResult(rowId, af)
			}
		}
	}
	return nil
}

// UpdateEntity 更新实体数据
func UpdateEntity(sess *xorm.Session, table string, ckv *query.ColumnKeyVal, dt *query.DataTable) error {
	if ckv.KCols == nil || len(ckv.KCols) == 0 {
		return fmt.Errorf("no primary column values provided")
	}
	bld := builder.Dialect(sess.Engine().DriverName())
	bld.From(table)

	pkCond, _, err := buildPrimaryKeyCondition(dt, ckv.KCols)
	if err != nil {
		return err
	}

	_, vals, err := fetchFirstRowValues(dt, ckv.VCols)
	if err != nil {
		return err
	}
	allIdx, err := dt.FetchColumnsIndex(ckv.VCols, ckv.KCols)
	if err != nil {
		return err
	}
	// pkIdx, pks, err := fetchFirstRowValues(dt, vCols)

	valCond := buildValueConditions(ckv.VCols, vals)
	bld.Update(valCond...)
	bld.Where(pkCond)

	sql, _, err := bld.ToSQL()
	if err != nil {
		return err
	}
	logger.Info("update entity", zap.String("entity", table),
		zap.String("sql", sql), zap.Any("kCols", ckv.KCols), zap.Any("vCols", ckv.VCols))
	return executeUpdate(sess, dt, sql, allIdx)
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

// fetchFirstRowValues 获取要更新的值
func fetchFirstRowValues(dt *query.DataTable, cols []string) ([]int, []any, error) {
	valIdx, err := dt.FetchColumnsIndex(cols, nil)
	if err != nil {
		return nil, nil, err
	}

	vals, err := dt.FetchRow(0, valIdx, nil)
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
		args, _ := dt.FetchRowDataWithSQL(i, valIdx, nil, sql)
		result, err := sess.Exec(args...)
		if err != nil {
			return err
		}
		af, _ := result.RowsAffected()
		_ = dt.UpdateAffectedResult(i, af) // 累计变更影响记录数
	}
	return nil
}
