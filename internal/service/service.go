package service

import (
	"reversedns/internal/config"
	"reversedns/internal/service/interfaces"
)

type Services struct {
	Config     *config.Config
	DNSService interfaces.DNSSrv
}

func NewService(
	config *config.Config,
	dnsService interfaces.DNSSrv,
) *Services {
	return &Services{
		Config:     config,
		DNSService: dnsService,
	}
}
