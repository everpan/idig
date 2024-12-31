package role

import (
	// "github.com/everpan/idig/pkg/config"
	"github.com/everpan/idig/pkg/core"
	"xorm.io/xorm"
)

type Role struct {
}

func (r *Role) TableName() string {
	return "idig_role"
}

func init() {
	// config.RegisterReloadConfigFunc()
	core.RegisterInitTableFunction(InitEntityTable)
}

func InitEntityTable(engine *xorm.Engine) error {
	return nil
}

// 控制访问
// role -> entity 角色 实体
// role -> attr_group 角色 实体属性组
// role -> column 角色，可访问的实体字段
// 黑名单/白名单模式 黑名单优先
