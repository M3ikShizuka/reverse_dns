package dns

import (
	"errors"
	"github.com/miekg/dns"
	"os"
	"reversedns/internal/service/data"
	"reversedns/pkg/logger"
	"syscall"
	"time"
)

type DNS interface {
	GetDNSInfo(address string, fqdn string) ([]data.DNSInfo, error)
}

type Client struct {
}

var _ DNS = &Client{}

func NewDNSClient() *Client {
	return &Client{}
}

// https://stackoverflow.com/questions/48980108/hostname-ip-ttl

func (d *Client) GetDNSInfo(address string, fqdn string) ([]data.DNSInfo, error) {
	client := dns.Client{}
	msg := dns.Msg{}
	msg.SetQuestion(fqdn, dns.TypeA)
	response, _, err := client.Exchange(&msg, address)
	if err != nil {
		switch {
		case errors.Is(err, syscall.ENETUNREACH):
			// No internet
			/*
				// Can't connect (no internet)
				err = {error | *net.OpError}
				 Op = {string} "dial"
				 Net = {string} "udp"
				 Source = {net.Addr} nil
				 Addr = {net.Addr | *net.UDPAddr}
				 Err = {error | *os.SyscallError}
				  Syscall = {string} "connect"
				  Err = {error | syscall.Errno} ENETUNREACH (101)
			*/
			return nil, err
		case os.IsTimeout(err),
			errors.Is(err, os.ErrDeadlineExceeded):
			// i/o timeout
			/*
				err = {error | *net.OpError}
				 Op = {string} "read"
				 Net = {string} "udp"
				 Source = {net.Addr | *net.UDPAddr}
				 Addr = {net.Addr | *net.UDPAddr}
				 Err = {error | *poll.DeadlineExceededError} i/o timeout
			*/
			// Second chance
			// logger.Warn("DNS i/o timeout SECOND CHANCE for fqdn: " + fqdn)
			response, _, err = client.Exchange(&msg, address)
			if err != nil {
				logger.Warn("DNS i/o timeout SECOND CHANCE FAILED for fqdn: " + fqdn + " error: " + dns.ErrFqdn.Error())
				// Failed to retrieve data for current fqdn from server 2 times in a row. Skip this record.
				// The data for it will be retrieved at the next call of the update handler,
				// which will occur immediately, since the current record in the database will remain unupdated.
				return nil, err
			}
		case errors.Is(err, dns.ErrFqdn):
			logger.Warn("DNS incorrect fqdn: " + fqdn + " error: " + dns.ErrFqdn.Error())
			return nil, dns.ErrFqdn
		default:
			return nil, err
		}
	}

	answerSize := len(response.Answer)
	if answerSize < 1 {
		return nil, nil
	}

	dnsInfos := make([]data.DNSInfo, 0, answerSize)
	for _, answer := range response.Answer {
		if a, ok := answer.(*dns.A); ok {
			dnsInfo := data.DNSInfo{
				A:         a.A.String(),
				Fqdn:      answer.Header().Name,
				CreatedAt: time.Now(),
				TTL:       answer.Header().Ttl,
			}
			dnsInfos = append(dnsInfos, dnsInfo)
		}
	}

	// Fix the memory size for data storage.
	var dnsInfoResult []data.DNSInfo
	if len(dnsInfos) != cap(dnsInfos) {
		dnsInfoResult = make([]data.DNSInfo, len(dnsInfos))
		copy(dnsInfoResult, dnsInfos)
	} else {
		dnsInfoResult = dnsInfos
	}

	return dnsInfoResult, nil
}
