package interfaces

import "reversedns/internal/service/data"

type DNS interface {
	GetDNSInfo(address string, fqdn string) ([]data.DNSInfo, error)
}
