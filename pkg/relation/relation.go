package relation

import "xorm.io/xorm"

// RelationType 定义表之间的关系类型
type RelationType int

const (
	HasOne RelationType = iota
	HasMany
	BelongsTo
	ManyToMany
)

// SQLResult 定义 SQL 查询结果
type SQLResult struct {
	SQL  string
	Args []interface{}
}

// Relation 定义表之间的关系
type Relation struct {
	SourceTable  string       // 源表名
	TargetTable  string       // 目标表名
	Type         RelationType // 关系类型
	ReferenceKey string       // 引用键（通常是主键）
	ForeignKey   string       // 外键
	JoinTable    string       // 用于多对多关系的中间表
}

// NewRelation 创建一个新的关系实例
func NewRelation() *Relation {
	return &Relation{}
}

// SetSourceTable 设置源表
func (r *Relation) SetSourceTable(table string) *Relation {
	r.SourceTable = table
	return r
}

// SetTargetTable 设置目标表
func (r *Relation) SetTargetTable(table string) *Relation {
	r.TargetTable = table
	return r
}

// SetType 设置关系类型
func (r *Relation) SetType(relationType RelationType) *Relation {
	r.Type = relationType
	return r
}

// SetForeignKey 设置外键
func (r *Relation) SetForeignKey(key string) *Relation {
	r.ForeignKey = key
	return r
}

// SetReferenceKey 设置引用键
func (r *Relation) SetReferenceKey(key string) *Relation {
	r.ReferenceKey = key
	return r
}

// SetJoinTable 设置连接表（用于多对多关系）
func (r *Relation) SetJoinTable(table string) *Relation {
	r.JoinTable = table
	return r
}

// XormEngine 适配器，实现 QueryBuilder 接口
type XormEngine struct {
	*xorm.Engine
}

// NewXormEngine 创建一个新的 XormEngine 适配器
func NewXormEngine(engine *xorm.Engine) *XormEngine {
	return &XormEngine{Engine: engine}
}
