package query

import (
	"fmt"
	"slices"

	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/goccy/go-json"
	"golang.org/x/exp/maps"
)

// 定义常量，避免魔法字符串
const (
	KeyCols = "cols"
	KeyVals = "vals"
)

// DataTable 表示一个二维数据表结构
// cols: 列名列表
// data: 行数据，每行包含与cols对应的值
type DataTable struct {
	cols []string // 列名列表
	data [][]any  // 行数据
}

// JDataTable JSON序列化结构
type JDataTable struct {
	Cols []string `json:"cols"`
	Data [][]any  `json:"vals"`
}

// From 从DataTable转换为JDataTable
func (jd *JDataTable) From(dt *DataTable) {
	jd.Cols, jd.Data = dt.cols, dt.data
}

// FromArrayMap 从map数组转换为JDataTable
func (jd *JDataTable) FromArrayMap(am []map[string]any) {
	if len(am) < 1 {
		return
	}
	// 预分配容量
	jd.Cols = make([]string, 0, len(am[0]))
	jd.Data = make([][]any, 0, len(am))

	idx := map[string]int{}
	for k := range am[0] {
		idx[k] = len(jd.Cols)
		jd.Cols = append(jd.Cols, k)
	}

	for i := range am {
		m := am[i]
		av := make([]any, len(jd.Cols))
		for k, v := range m {
			j, ok := idx[k]
			if !ok {
				j = len(jd.Cols)
				idx[k] = j
				jd.Cols = append(jd.Cols, k)
			}
			av[j] = v
		}
		jd.Data = append(jd.Data, av)
	}
}

// NewDataTable 创建新的DataTable实例
func NewDataTable() *DataTable {
	return &DataTable{
		cols: make([]string, 0),
		data: make([][]any, 0),
	}
}

// Clear 清理数据表中的所有数据
func (dt *DataTable) Clear() {
	dt.cols = dt.cols[:0]
	dt.data = dt.data[:0]
}

// ParseKeyCols 解析列名
func (dt *DataTable) ParseKeyCols(raw map[string]any) error {
	if v, ok := raw[KeyCols]; ok {
		cols, ok := v.([]any)
		if !ok {
			return fmt.Errorf("invalid cols type: expected []any, got %T", v)
		}
		for _, v1 := range cols {
			s, ok := v1.(string)
			if !ok {
				return fmt.Errorf("cols value '%v' type is '%T', need 'string' type", v1, v1)
			}
			dt.AddColumn(s)
		}
	}
	return nil
}

// ParseKeyVals 解析数据值
func (dt *DataTable) ParseKeyVals(raw map[string]any) error {
	v, ok := raw[KeyVals]
	if !ok {
		return nil
	}

	switch r := v.(type) {
	case map[string]any:
		return dt.parseSingleObject(r)
	case []any:
		return dt.parseMultiValues(r)
	default:
		return fmt.Errorf("parse vals error: invalid value type: %T", r)
	}
}

// parseSingleObject 解析单个对象
func (dt *DataTable) parseSingleObject(obj map[string]any) error {
	dt.AddColumns(maps.Keys(obj))
	rowData, err := parseSingleValue(dt.Columns(), obj)
	if err != nil {
		return fmt.Errorf("parse single value error: %s", err.Error())
	}
	return dt.AddRow(rowData)
}

// parseMultiValues 解析多个值
func (dt *DataTable) parseMultiValues(vals []any) error {
	for i, a := range vals {
		switch r1 := a.(type) {
		case []any:
			if err := dt.AddRow(r1); err != nil {
				return err
			}
		case map[string]any:
			if i == 0 {
				dt.AddColumns(maps.Keys(r1))
			}
			rowData, err := parseSingleValue(dt.Columns(), r1)
			if err != nil {
				return fmt.Errorf("parse single value error: %s", err.Error())
			}
			if err := dt.AddRow(rowData); err != nil {
				return err
			}
		default:
			return fmt.Errorf("parse multi vals error: need array vals, not %T", r1)
		}
	}
	return nil
}

// ParseValues 从JSON字节数据解析
func (dt *DataTable) ParseValues(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("json unmarshal error: %v", err.Error())
	}
	if err := dt.ParseKeyCols(raw); err != nil {
		return err
	}
	return dt.ParseKeyVals(raw)
}

// parseSingleValue 解析单个值
func parseSingleValue(colList []string, mv map[string]any) ([]any, error) {
	if mv == nil {
		return nil, fmt.Errorf("input map is nil")
	}

	ret := make([]any, len(colList))
	for i, col := range colList {
		v, ok := mv[col]
		if !ok {
			return nil, fmt.Errorf("column '%s' not found", col)
		}
		ret[i] = v
	}
	return ret, nil
}

// AddColumn 添加列
func (dt *DataTable) AddColumn(col string) int {
	if col == "" {
		return -1
	}

	idx := slices.Index(dt.cols, col)
	if idx > -1 {
		return idx
	}

	idx = len(dt.cols)
	dt.cols = append(dt.cols, col)

	// 为现有行添加新列
	if len(dt.data) > 0 {
		for i := range dt.data {
			dt.data[i] = append(dt.data[i], nil)
		}
	}
	return idx
}

// AddColumns 批量添加列
func (dt *DataTable) AddColumns(cols []string) {
	for _, col := range cols {
		dt.AddColumn(col)
	}
}

// AddRow 添加行数据
func (dt *DataTable) AddRow(row []any) error {
	if row == nil {
		return fmt.Errorf("row cannot be nil")
	}
	if len(row) != len(dt.cols) {
		return fmt.Errorf("row len:%d is not equal to col len:%d", len(row), len(dt.cols))
	}
	dt.data = append(dt.data, row)
	return nil
}

// Columns 获取列名列表
func (dt *DataTable) Columns() []string {
	return dt.cols
}

// Values 获取所有行数据
func (dt *DataTable) Values() [][]any {
	return dt.data
}

// ValidIndex 验证索引是否有效
func (dt *DataTable) ValidIndex(index []int) error {
	if len(index) == 0 {
		return fmt.Errorf("index array is empty")
	}

	maxIndex := slices.Max(index)
	if maxIndex >= len(dt.cols) {
		return fmt.Errorf("max index %d is greater than col len %d", maxIndex, len(dt.cols))
	}

	minIndex := slices.Min(index)
	if minIndex < 0 {
		return fmt.Errorf("min index %d is less than 0", minIndex)
	}
	return nil
}

// FetchRowData 通过索引获取指定行数据
func (dt *DataTable) FetchRowData(row int, index []int) ([]any, error) {
	if err := dt.CheckRowId(row); err != nil {
		return nil, err
	}
	if err := dt.ValidIndex(index); err != nil {
		return nil, err
	}

	result := make([]any, len(index))
	data := dt.data[row]
	for i, j := range index {
		result[i] = data[j]
	}
	return result, nil
}

// FetchRowDataWithSQL 获取行数据，并在头部放入sqlStr
func (dt *DataTable) FetchRowDataWithSQL(row int, index []int, sqlStr string) ([]any, error) {
	data, err := dt.FetchRowData(row, index)
	if err != nil {
		return nil, err
	}

	result := make([]any, len(index)+1)
	result[0] = sqlStr
	copy(result[1:], data)
	return result, nil
}

// FetchColumnIndex 获取指定列的索引
func (dt *DataTable) FetchColumnIndex(col string) int {
	if col == "" {
		return -1
	}
	return slices.Index(dt.cols, col)
}

// FetchColumnsIndex 获取指定列的索引列表
func (dt *DataTable) FetchColumnsIndex(cols []string) ([]int, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("columns list is empty")
	}

	index := make(map[string]int, len(dt.cols))
	for i, col := range dt.cols {
		index[col] = i
	}

	result := make([]int, 0, len(cols))
	for _, col := range cols {
		if i, ok := index[col]; !ok {
			return nil, fmt.Errorf("column %s not found", col)
		} else {
			result = append(result, i)
		}
	}
	return result, nil
}

// FetchRowDataByColumns 通过列名获取指定行数据
// 注意：多次获取时，建议使用 FetchRowData 以获得更好的性能
func (dt *DataTable) FetchRowDataByColumns(row int, cols []string) ([]any, error) {
	index, err := dt.FetchColumnsIndex(cols)
	if err != nil {
		return nil, err
	}
	return dt.FetchRowData(row, index)
}

// SortColumnsAndFetchIndices 列排序，且获取索引
func (dt *DataTable) SortColumnsAndFetchIndices(cols []string) ([]int, error) {
	if len(cols) == 0 {
		return nil, fmt.Errorf("columns list is empty")
	}
	slices.Sort(cols)
	return dt.FetchColumnsIndex(cols)
}

// CheckRowId 检查行ID是否有效
func (dt *DataTable) CheckRowId(rowId int) error {
	if rowId < 0 || rowId >= len(dt.data) {
		return fmt.Errorf("row %d is out of range [0, %d)", rowId, len(dt.data))
	}
	return nil
}

// CheckColId 检查列ID是否有效
func (dt *DataTable) CheckColId(colId int) error {
	if colId < 0 || colId >= len(dt.cols) {
		return fmt.Errorf("column %d is out of range [0, %d)", colId, len(dt.cols))
	}
	return nil
}

// CheckRowColId 检查行列ID是否有效
func (dt *DataTable) CheckRowColId(rowId, colId int) error {
	if err := dt.CheckRowId(rowId); err != nil {
		return err
	}
	return dt.CheckColId(colId)
}

// UpdateData 更新指定位置的数据
func (dt *DataTable) UpdateData(rowId, colId int, d any) error {
	if err := dt.CheckRowColId(rowId, colId); err != nil {
		return err
	}
	dt.data[rowId][colId] = d
	return nil
}

// addColumnToTable 将列添加到指定表
func (dt *DataTable) addColumnToTable(col, tableName string, ret map[string][]string) {
	if cols, exists := ret[tableName]; exists {
		ret[tableName] = append(cols, col)
	} else {
		ret[tableName] = []string{col}
	}
}

// DivisionColumnsToTable 将列按表进行分组
func (dt *DataTable) DivisionColumnsToTable(m *meta.EntityMeta, withPk bool) (map[string][]string, error) {
	if m == nil {
		return nil, fmt.Errorf("EntityMeta is nil")
	}

	ret := make(map[string][]string)
	pk := m.PrimaryColumn()

	// 处理所有列
	for _, col := range dt.cols {
		if !withPk && pk == col {
			continue
		}
		if m1, ok := m.ColumnIndex[col]; ok {
			dt.addColumnToTable(col, m1.TableName, ret)
		} else {
			return nil, fmt.Errorf("column '%s' not found", col)
		}
	}

	// 处理主键
	if withPk {
		primaryTable := m.PrimaryTable()
		for t := range ret {
			if t != primaryTable {
				dt.addColumnToTable(pk, t, ret)
			}
		}
	}

	return ret, nil
}
