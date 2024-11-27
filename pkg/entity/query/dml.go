package query

import (
	"fmt"
	"github.com/goccy/go-json"
)

/*
	"cols":[],
	"values":[
		[],[]
	]
*/
/*
	"values":{
		"col-name":"values",
		"enable":1,
	}
*/
/*
	"values":[{
		"col-name":"values",
		"enable":1,
	},{
		"col-name":"values",
		"enable":1,
	}]
*/

type DmlValues struct {
	cols   []string
	values [][]any
}

func parseValues(data []byte) error {
	dmlVal := &DmlValues{}
	var (
		raw map[string]any
	)
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}
	for k, v := range raw {
		if k == "cols" {
			fmt.Printf("cols : %T %v\n", v, v)
			//for _, s := range v.([]any) {
			//	dmlVal.cols = append(dmlVal.cols, s.(string))
			//}
			// dmlVal.cols = v.([]string)
			continue
		}
		fmt.Printf("%s : %T %v\n", k, v, v)
		switch r := v.(type) {
		case map[string]any:
			tmp, err1 := parseSignalValue(dmlVal.cols, r)
			if err1 != nil {
				return err1
			} else {
				dmlVal.values = append(dmlVal.values, tmp)
			}
		case []map[string]any:
			// multi-values
			dmlVal.values, err = parseMultiValues(dmlVal.cols, r)
		case [][]any:
			dmlVal.values = r
		default:
			return fmt.Errorf("invalid value type: %T", r)
		}
	}
	if err != nil {
		return err
	}
	//for _, v := range values {
	//	values = append(values, v)
	//}
	return nil
}

func parseSignalValue(colList []string, mv map[string]any) ([]any, error) {
	var ret []any
	if len(colList) == 0 {
		// 首行处理
		for c, v := range mv {
			colList = append(colList, c)
			ret = append(ret, v)
		}
		return ret, nil
	}
	for _, col := range colList {
		v, ok := mv[col]
		if !ok {
			return nil, fmt.Errorf("column '%s' not found", col)
		}
		ret = append(ret, v)
	}
	return ret, nil
}

func parseMultiValues(colList []string, mvs []map[string]any) ([][]any, error) {
	var ret [][]any
	for _, mv := range mvs {
		r, err := parseSignalValue(colList, mv)
		if err != nil {
			return nil, err
		}
		ret = append(ret, r)
	}
	return ret, nil
}

//func parseArrayValues(colList []string, mvs [][]any) ([][]any, error) {
//	return mvs,nil
//}
