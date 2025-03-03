package relation

import (
	"xorm.io/builder"
)

// ToSQL 生成关系对应的 SQL 语句
func (r *Relation) ToSQL() (*SQLResult, error) {
	switch r.Type {
	case HasOne, BelongsTo:
		return r.buildOneToOneSQL()
	case HasMany:
		return r.buildOneToManySQL()
	case ManyToMany:
		return r.buildManyToManySQL()
	default:
		return r.buildSimpleSQL()
	}
}

// buildSimpleSQL 构建简单查询
func (r *Relation) buildSimpleSQL() (*SQLResult, error) {
	b := builder.Select("*").From(r.SourceTable)
	sql, args, err := b.ToSQL()
	if err != nil {
		return nil, err
	}
	return &SQLResult{SQL: sql, Args: args}, nil
}

// buildOneToOneSQL 构建一对一查询
func (r *Relation) buildOneToOneSQL() (*SQLResult, error) {
	joinCond := builder.Eq{
		r.SourceTable + "." + r.ReferenceKey: builder.Expr(r.TargetTable + "." + r.ForeignKey),
	}

	b := builder.Select("*").
		From(r.SourceTable).
		LeftJoin(r.TargetTable, joinCond)

	sql, args, err := b.ToSQL()
	if err != nil {
		return nil, err
	}
	return &SQLResult{SQL: sql, Args: args}, nil
}

// buildOneToManySQL 构建一对多查询
func (r *Relation) buildOneToManySQL() (*SQLResult, error) {
	joinCond := builder.Eq{
		r.SourceTable + "." + r.ReferenceKey: builder.Expr(r.TargetTable + "." + r.ForeignKey),
	}

	b := builder.Select("*").
		From(r.SourceTable).
		LeftJoin(r.TargetTable, joinCond)

	sql, args, err := b.ToSQL()
	if err != nil {
		return nil, err
	}
	return &SQLResult{SQL: sql, Args: args}, nil
}

// buildManyToManySQL 构建多对多查询
func (r *Relation) buildManyToManySQL() (*SQLResult, error) {
	joinTableCond := builder.Eq{
		r.SourceTable + "." + r.ReferenceKey: builder.Expr(r.JoinTable + "." + r.SourceTable + "_id"),
	}

	targetTableCond := builder.Eq{
		r.JoinTable + "." + r.TargetTable + "_id": builder.Expr(r.TargetTable + "." + r.ReferenceKey),
	}

	b := builder.Select("*").
		From(r.SourceTable).
		LeftJoin(r.JoinTable, joinTableCond).
		LeftJoin(r.TargetTable, targetTableCond)

	sql, args, err := b.ToSQL()
	if err != nil {
		return nil, err
	}
	return &SQLResult{SQL: sql, Args: args}, nil
}
