package interfaces

import (
	"context"
	"reversedns/internal/service/data"
	"time"
)

type DNSSrv interface {
	CancelWorkers()
	SchedulerUpdateTrigger(ctx context.Context) error
	ProcessDNSDataSchedulerCallback(ctx context.Context, operationStartTimePtr *time.Time, lastUpdateTimePtr *time.Time) error
	ProcessNewlyReceivedDNSData(fqdns []string) error
	GetDomainsByIP(ctx context.Context, ips []string) (data.IPDomainsData, error)
}
