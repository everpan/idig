package config

import (
	"fmt"
	"sync"

	"github.com/spf13/viper"
	"xorm.io/xorm"
)

type Tenant struct {
	TenantIdx   uint32 `json:"tenant_idx" xorm:"pk autoincr"` //tenant_id 参与自动过滤
	TenantUid   string `json:"tenant_uid" xorm:"unique"`      //uuid
	Name        string `json:"name"`
	CnName      string `json:"cn_name"`
	Driver      string `json:"driver"`
	DataSource  string `json:"data_source"`
	ExtendInfo  string `json:"extend_info"`
	Environment string `json:"environment"` // host test normal
	Status      int    `json:"status"`
}

func (t *Tenant) TableName() string {
	return "idig_tenant"
}

func InitTable(engine *xorm.Engine) error {
	err := engine.Sync2(new(Tenant))
	if err != nil {
		return err
	}
	_, _ = engine.Insert(DefaultTenant)
	// entity.RegisterEntity(engine, "tenant", "租户信息", (&Tenant{}).TableName(), "tenant_id")
	return nil
}

func init() {
	RegisterInitTableFunction(InitTable)
	viper.SetDefault("tenant.default.driver", DefaultTenant.Driver)
	viper.SetDefault("tenant.default.data-source", DefaultTenant.DataSource)
	viper.SetDefault("tenant.http-header-key", TenantHeader)
	RegisterReloadConfigFunc(ReloadTenantConfig)
}

func ReloadTenantConfig() error {
	DefaultTenant.Driver = viper.GetString("tenant.default.driver")
	DefaultTenant.DataSource = viper.GetString("tenant.default.data-source")
	TenantHeader = viper.GetString("tenant.http-header-key")
	tenantCache.Store(DefaultTenant.TenantUid, DefaultTenant)
	return nil
}

var (
	DefaultTenant = &Tenant{
		TenantIdx:  1,
		TenantUid:  "69515562-5192-49aa-b223-b0953d83c887",
		Name:       "default",
		CnName:     "默认租户",
		Driver:     "sqlite3",
		DataSource: "/tmp/tenant_test.db",
		Status:     1,
	}
	TenantHeader = "X-Tenant-UID"
	tenantCache  = sync.Map{}
)

func (t *Tenant) CreateEngine() (*xorm.Engine, error) {
	return xorm.NewEngine(t.Driver, t.DataSource)
}

func GetFromCache(uid string) *Tenant {
	if v, ok := tenantCache.Load(uid); ok {
		return v.(*Tenant)
	}
	return nil
}

func GetFromDBThenCached(uid string, engine *xorm.Engine) (*Tenant, error) {
	if engine == nil {
		return DefaultTenant, nil
	}
	t := &Tenant{
		TenantUid: uid, Status: 1,
	}
	has, err := engine.Get(t)
	if err != nil {
		return nil, err
	}
	if !has {
		return nil, fmt.Errorf("tenant:%s not exist or status != 1", uid)
	}
	tenantCache.Store(uid, t)
	return t, nil
}
