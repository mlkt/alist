package kopia

import (
	"github.com/alist-org/alist/v3/internal/driver"
	"github.com/alist-org/alist/v3/internal/op"
)

type Addition struct {
	Url      string `json:"url" required:"true"`
	UserName string `json:"userName" required:"true"`
	Host     string `json:"host" required:"true"`
	Path     string `json:"path" required:"true"`
	Snapshot string `json:"snapshot" required:"false" default:"latest"`
}

var config = driver.Config{
	Name:        "Kopia",
	LocalSort:   true,
	DefaultRoot: "-1",
	OnlyLocal:   true,
}

func init() {
	op.RegisterDriver(func() driver.Driver {
		return &Kopia{}
	})
}
