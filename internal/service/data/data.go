package data

import (
	"encoding/json"
	"time"
)

type DNSInfo struct {
	A         string
	Fqdn      string
	CreatedAt time.Time
	TTL       uint32 // in seconds
}

type DNSFqdnA struct {
	A    string `bson:"a"`
	Fqdn string `bson:"fqdn"`
}

type IPDomains struct {
	IP      string
	Domains []string
}

type IPDomainsData []IPDomains

func (d IPDomainsData) MarshalJSON() ([]byte, error) {
	data := map[string][]string{}

	for _, ipDomain := range d {
		data[ipDomain.IP] = ipDomain.Domains
	}

	return json.MarshalIndent(data, "", "  ")
}

type UUID [16]byte
