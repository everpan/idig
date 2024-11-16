package config

import (
	"fmt"
	"xorm.io/xorm"
)

type Tenant struct {
	TenantId    uint32 `json:"tenant_id" xorm:"pk autoincr"`
	TenantUid   string `json:"tenant_uid" xorm:"unique"` //uuid
	Name        string `json:"name"`
	CnName      string `json:"cn_name"`
	Driver      string `json:"driver"`
	DataSource  string `json:"data_source"`
	ExtendInfo  string `json:"extend_info"`
	Environment string `json:"environment"` // host test normal
}

func InitTable(engine *xorm.Engine) error {
	err := engine.Sync2(new(Tenant))
	if err != nil {
		return err
	}
	engine.Insert(DefaultTenant)
	return nil
}

func init() {
	RegisterInitTableFunction(InitTable)
}

var DefaultTenant = &Tenant{
	TenantId:   1,
	TenantUid:  "69515562-5192-49aa-b223-b0953d83c887",
	Name:       "default",
	CnName:     "默认租户",
	Driver:     "sqlite3",
	DataSource: "/tmp/tenant_test.db",
	ExtendInfo: "test",
}

func (t *Tenant) CreateEngine() (*xorm.Engine, error) {
	return xorm.NewEngine(t.Driver, t.DataSource)
}

func Get(uid string, engine *xorm.Engine) (*Tenant, error) {
	if engine == nil {
		return DefaultTenant, nil
	}
	t := &Tenant{
		TenantUid: uid,
	}
	has, err := engine.Get(t)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, fmt.Errorf("tenant:%s not exist", uid)
	}
	return t, nil
}
