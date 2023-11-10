package interfaces

import (
	"context"
	"time"
)

type DNSRepository interface {
	IsCollectionEmpty(ctx context.Context) (bool, error)
	InitDataBaseStructure(ctx context.Context) error
	GetDomainsByIP(ctx context.Context, ip string) ([]string, error)
	GetDataRequiringUpdate(ctx context.Context, operationStartTimePtr *time.Time, lastUpdateTimePtr *time.Time) ([]string, error)
	UpdateDNS(ctx context.Context, records []interface{}, recordLifetime []interface{}, operationStartTimePtr *time.Time) error
	DoTransaction(ctx context.Context, handler func(sctx context.Context) error) error
	GetNearTimeOfObsoleteRecord(ctx context.Context) (*time.Time, error)
}
