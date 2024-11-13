package core

import (
	"github.com/everpan/idig/pkg/config"
	"xorm.io/xorm"
)

type Tenant struct {
	TenantId    uint32 `json:"tenant_id" xorm:"pk autoincr"`
	TenantUid   string `json:"tenant_uid"`
	Name        string `json:"name"`
	ExtendInfo  string `json:"extend_info"`
	Environment string `json:"environment"` // host test normal
}

func InitTable(engine *xorm.Engine) error {
	return engine.Sync2(new(Tenant))
}

func init() {
	config.RegisterInitTableFunction(InitTable)
}
