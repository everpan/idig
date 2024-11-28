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

func (dmlVal *DmlValues) Reset() {
	dmlVal.cols = nil
	dmlVal.values = nil
}

func (dmlVal *DmlValues) ParseValues(data []byte) error {
	var (
		raw map[string]any
	)
	err := json.Unmarshal(data, &raw)
	if err != nil {
		return err
	}

	for k, v := range raw {
		fmt.Printf("key %s, raw type %T\n", k, v)
		if k == "cols" {
			for _, v1 := range v.([]any) {
				s, ok := v1.(string)
				if !ok {
					return fmt.Errorf("cols value '%v' type is '%T',need 'string' type", v1, v1)
				}
				dmlVal.cols = append(dmlVal.cols, s)
			}
			continue
		}
		switch r := v.(type) {
		case map[string]any:
			// single value
			dmlVal.acquireColumnKeyFromFirstValues(r)
			tmp, err1 := parseSingleValue(dmlVal.cols, r)
			if err1 != nil {
				return fmt.Errorf("parse single value error:%s", err1.Error())
			} else {
				dmlVal.values = append(dmlVal.values, tmp)
			}
		case []any:
			for i, a := range r {
				switch r1 := a.(type) {
				case []any:
					dmlVal.values = append(dmlVal.values, r1)
				case map[string]any:
					// multi obj values
					if i == 0 {
						dmlVal.acquireColumnKeyFromFirstValues(r1)
					}
					tmp, err1 := parseSingleValue(dmlVal.cols, r1)
					if err1 != nil {
						return fmt.Errorf("parse single value error:%s", err1.Error())
					} else {
						dmlVal.values = append(dmlVal.values, tmp)
					}
				default:
					return fmt.Errorf("parse multi values error:need array values,not %T", r1)
				}
			}
		default:
			return fmt.Errorf("parse values error:invalid value type: %T", r)
		}
	}
	//for _, v := range values {
	//	values = append(values, v)
	//}
	return nil
}

func (dmlVal *DmlValues) acquireColumnKeyFromFirstValues(mv map[string]any) {
	for k := range mv {
		dmlVal.cols = append(dmlVal.cols, k)
	}
}

func parseSingleValue(colList []string, mv map[string]any) ([]any, error) {
	var ret []any
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
		r, err := parseSingleValue(colList, mv)
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
