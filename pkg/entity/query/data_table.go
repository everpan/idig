package query

import (
	"fmt"
	"slices"

	"github.com/everpan/idig/pkg/entity/meta"
	"github.com/goccy/go-json"
	"golang.org/x/exp/maps"
)

type DataTable struct {
	cols []string
	data [][]any
}

type JDataTable struct {
	Cols []string `json:"cols"`
	Data [][]any  `json:"vals"`
}

func (jd *JDataTable) From(dt *DataTable) {
	jd.Cols, jd.Data = dt.cols, dt.data
}

func (jd *JDataTable) FromArrayMap(am []map[string]any) {
	if len(am) < 1 {
		return
	}
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
	}
}

func NewDataTable() *DataTable {
	return &DataTable{}
}

func (dt *DataTable) ParseKeyCols(raw map[string]any) error {
	if v, ok := raw["cols"]; ok {
		for _, v1 := range v.([]any) {
			s, ok := v1.(string)
			if !ok {
				return fmt.Errorf("cols value '%v' type is '%T',need 'string' type", v1, v1)
			}
			dt.AddColumn(s)
		}
	}
	return nil
}

func (dt *DataTable) ParseKeyVals(raw map[string]any) error {
	var err error
	if v, ok := raw["vals"]; ok {
		switch r := v.(type) {
		case map[string]any:
			// single value
			dt.AddColumns(maps.Keys(r))
			if rowData, err1 := parseSingleValue(dt.Columns(), r); err1 != nil {
				return fmt.Errorf("parse single value error:%s", err1.Error())
			} else {
				if err = dt.AddRow(rowData); err != nil {
					return err
				}
			}
		case []any:
			for i, a := range r {
				switch r1 := a.(type) {
				case []any:
					if err = dt.AddRow(r1); err != nil {
						return err
					}
				case map[string]any:
					// multi obj vals
					if i == 0 {
						dt.AddColumns(maps.Keys(r1))
					}
					if rowData, err1 := parseSingleValue(dt.Columns(), r1); err1 != nil {
						return fmt.Errorf("parse single value error:%s", err1.Error())
					} else {
						if err = dt.AddRow(rowData); err != nil {
							return err
						}
					}
				default:
					return fmt.Errorf("parse multi vals error:need array vals,not %T", r1)
				}
			}
		default:
			return fmt.Errorf("parse vals error:invalid value type: %T", r)
		}
	}
	return nil
}

func (dt *DataTable) ParseValues(data []byte) error {
	var raw map[string]any
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("json umarshal error: %v", err.Error())
	}
	if err := dt.ParseKeyCols(raw); err != nil {
		return err
	}
	if err := dt.ParseKeyVals(raw); err != nil {
		return err
	}
	return nil
}

func parseSingleValue(colList []string, mv map[string]any) ([]any, error) {
	var ret = make([]any, len(colList))
	for i, col := range colList {
		v, ok := mv[col]
		if !ok {
			return nil, fmt.Errorf("column '%s' not found", col)
		}
		ret[i] = v
	}
	return ret, nil
}

func (dt *DataTable) AddColumn(col string) int {
	idx := slices.Index(dt.cols, col)
	if idx > -1 {
		return idx
	}
	idx = len(dt.cols)
	dt.cols = append(dt.cols, col)
	if len(dt.data) > 0 {
		for i := range dt.data {
			dt.data[i] = append(dt.data[i], nil)
		}
	}
	return idx
}

func (dt *DataTable) AddColumns(cols []string) {
	for _, col := range cols {
		dt.AddColumn(col)
	}
}

func (dt *DataTable) AddRow(row []any) error {
	if len(row) != len(dt.cols) {
		return fmt.Errorf("row len:%d is not equal to col len:%d", len(row), len(dt.cols))
	}
	dt.data = append(dt.data, row)
	return nil
}

func (dt *DataTable) Columns() []string {
	return dt.cols
}

func (dt *DataTable) Values() [][]any {
	return dt.data
}

func (dt *DataTable) ValidIndex(index []int) error {
	maxIndex := slices.Max(index)
	if maxIndex >= len(dt.cols) {
		return fmt.Errorf("max index is greater than col len")
	}
	minIndex := slices.Min(index)
	if minIndex < 0 {
		return fmt.Errorf("min index is less than 0")
	}
	return nil
}

// FetchRowData 通过索引获取指定行数据
func (dt *DataTable) FetchRowData(row int, index []int) []any {
	var result = make([]any, len(index))
	data := dt.data[row]
	for i, j := range index {
		result[i] = data[j]
	}
	return result
}

// FetchRowDataWithSQL 获取行数据，并在头部放入sqlStr
func (dt *DataTable) FetchRowDataWithSQL(row int, index []int, sqlStr string) []any {
	var result = make([]any, len(index)+1)
	result[0] = sqlStr
	data := dt.data[row]
	for i, j := range index {
		result[i+1] = data[j]
	}
	return result
}

// FetchColumnIndex 获取制定列的索引
func (dt *DataTable) FetchColumnIndex(col string) int {
	return slices.Index(dt.cols, col)
}

// FetchColumnsIndex 获取制定列的索引
func (dt *DataTable) FetchColumnsIndex(cols []string) ([]int, error) {
	index := map[string]int{}
	for i, col := range dt.cols {
		index[col] = i
	}
	var result []int
	for _, col := range cols {
		if i, ok := index[col]; !ok {
			return nil, fmt.Errorf("column %s not found", col)
		} else {
			result = append(result, i)
		}
	}
	return result, nil
}

// FetchRowDataByColumns 通过列名获取指定行数据，多次获取，效率不高；请使用 FetchRowData
func (dt *DataTable) FetchRowDataByColumns(row int, cols []string) ([]any, error) {
	index, err := dt.FetchColumnsIndex(cols)
	if err != nil {
		return nil, err
	}
	return dt.FetchRowData(row, index), nil
}

// SortColumnsAndFetchIndices 列排序，且获取索引
func (dt *DataTable) SortColumnsAndFetchIndices(cols []string) ([]int, error) {
	slices.Sort(cols)
	return dt.FetchColumnsIndex(cols)
}

func (dt *DataTable) CheckRowColId(rowId, colId int) error {
	if rowId < 0 || rowId >= len(dt.data) {
		return fmt.Errorf("row %d is out of range", rowId)
	}
	if colId < 0 || colId >= len(dt.cols) {
		return fmt.Errorf("column %d is out of range", colId)
	}
	return nil
}

func (dt *DataTable) UpdateData(rowId, colId int, d any) {
	dt.data[rowId][colId] = d
}

func (dt *DataTable) DivisionColumnsToTable(m *meta.EntityMeta, withPk bool) (map[string][]string, error) {
	var ret = map[string][]string{}
	pk := m.PrimaryColumn()
	for _, col := range dt.cols {
		if !withPk && pk == col {
			continue
		}
		if m1, ok := m.ColumnIndex[col]; ok {
			if _, ok2 := ret[m1.TableName]; ok2 {
				ret[m1.TableName] = append(ret[m1.TableName], col)
			} else {
				dt1 := []string{col}
				ret[m1.TableName] = dt1
			}
		} else {
			return nil, fmt.Errorf("column '%s' not found", col)
		}
	}
	if withPk {
		for t := range ret {
			if t != m.PrimaryTable() {
				ret[t] = append(ret[t], pk)
			}
		}
	}
	return ret, nil
}
