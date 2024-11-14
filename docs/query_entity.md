# 实体场景描述

## 查询meta
作为一个用户，必须了解实体及实体属性
便于观察实体信息相关的属性
需要进行实体相关的信息查询
例如：
GET /xpath/entity/meta/{entity_name}
```json5
{
  "entity": "user",
  "attrs": [ // 属性
    {
      "name": "user_idx",
      "type": "integer",
      "attr_table": "user"
    },
    {
      "name": "name",
      "type": "text",
      "attr_table": "user"
    },
    {
      "name": "user_idx",
      "type": "integer",
      "attr_table": "user_department"
    },
    {
      "name": "dept_name",
      "type": "text",
      "attr_table": "user_department"
    }
  ],
  "entry_info": {
    "entity_idx": 1,
    "entity_name": "user",
    "desc": "",
    "pk_attr_table": "user",
    "pk_attr_field": "user_idx",
    "status": 1
  },
  "group_info": [
    {
      "group_idx": 1,
      "attr_table": "user",
      "group_name": "User base",
      "desc": ""
    },
    {
      "group_idx": 2,
      "attr_table": "user_department",
      "group_name": "",
      "desc": ""
    }
  ],
  "primary_keys": [
    "user_idx"
  ]
}
```
## 查询数据
```json5
{
  query: [
    {
      "user": {
        col: [
          "idx",
          "name",
          {
            "col": "age",
            "alias": "nl"
          }
        ],
        where: [
          {
            "col": "name",
            "op": "eq",
            "val": "ever"
          },
          {
            "col": "age",
            "val": "30",
            "op": "lt",
            "mode": "or"
          }
        ],
        "order": [
          {}
        ]
      }
    },
  ],
}
```