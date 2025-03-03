package relation

import (
	"fmt"
	"testing"
)

func ExampleRelation() {
	// 创建一个新的关系实例
	relation := NewRelation()

	// 示例1：一对一关系（用户和用户详情）
	userRelation := relation.
		SetSourceTable("rf_user").
		SetTargetTable("user_profiles").
		SetType(HasOne).
		SetReferenceKey("id").
		SetForeignKey("user_id")

	// 生成 SQL
	sql1, err := userRelation.ToSQL()
	if err != nil {
		panic(err)
	}
	fmt.Printf("One-to-One SQL: %s\nArgs: %v\n", sql1.SQL, sql1.Args)

	// 示例2：一对多关系（用户和订单）
	orderRelation := relation.
		SetSourceTable("users").
		SetTargetTable("orders").
		SetType(HasMany).
		SetReferenceKey("id").
		SetForeignKey("user_id")

	// 生成 SQL
	sql2, err := orderRelation.ToSQL()
	if err != nil {
		panic(err)
	}
	fmt.Printf("One-to-Many SQL: %s\nArgs: %v\n", sql2.SQL, sql2.Args)

	// 示例3：多对多关系（用户和角色）
	roleRelation := relation.
		SetSourceTable("users").
		SetTargetTable("roles").
		SetType(ManyToMany).
		SetReferenceKey("id").
		SetJoinTable("user_roles")

	// 生成 SQL
	sql3, err := roleRelation.ToSQL()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Many-to-Many SQL: %s\nArgs: %v\n", sql3.SQL, sql3.Args)
}

func TestRelation(t *testing.T) {
	// 这里可以添加实际的测试用例
	relation := NewRelation()

	// 测试一对一关系
	oneToOne := relation.
		SetSourceTable("users").
		SetTargetTable("profiles").
		SetType(HasOne).
		SetReferenceKey("id").
		SetForeignKey("user_id")

	result, err := oneToOne.ToSQL()
	if err != nil {
		t.Errorf("Failed to generate SQL: %v", err)
	}

	if result.SQL == "" {
		t.Error("Generated SQL should not be empty")
	}
}
