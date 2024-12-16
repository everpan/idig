# Entity Insert
## 基本数据结构
data table
用来描述一个数据表
cols列名，为一个数组
vals 多行数据，二维数组，第一维度为行，第二维为对应列值
    每行一个数据，与cols对应
例如：
```json5
    {
        "cols": ["col1","col2","intCol"],
        "vals": [["v11","v12",11],["v21","v22",22]]
    }
```
与下列结构等效
```json5
    {
        "vals":[
            {"col1":"v11","col2":"v12","intCol":11},
            {"col1":"v21","col2":"v22","intCol":22},
        ]
    }
```
当仅有一行数据时候，vals可以表示为单个obj，如
```json5
    {
        "vals":{"col1":"v11","col2":"v12","intCol":11}
    }
```