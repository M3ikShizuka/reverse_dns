package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"reversedns/internal/config"
	"reversedns/internal/domain"
	"reversedns/internal/infrastructure/contextinf"
	"reversedns/internal/infrastructure/uuid"
	"reversedns/internal/service/data"
	"reversedns/internal/service/interfaces"
	"reversedns/internal/service/parallelize"
	"reversedns/internal/service/scheduler"
	"reversedns/pkg/logger"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type IpInfo struct {
	IP      string
	Domains []string
}

type cancelContext struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type DNSService struct {
	dns        interfaces.DNS
	DNSRepo    interfaces.DNSRepository
	Scheduler  *scheduler.Scheduler
	config     *config.Config
	contexts   map[data.UUID]*cancelContext
	mxContexts sync.Mutex
}

var _ interfaces.DNSSrv = &DNSService{}

func NewDNSService(dns interfaces.DNS, dnsRepo interfaces.DNSRepository, scheduler *scheduler.Scheduler, config *config.Config) *DNSService {
	return &DNSService{
		dns:       dns,
		DNSRepo:   dnsRepo,
		Scheduler: scheduler,
		config:    config,
		contexts:  make(map[data.UUID]*cancelContext),
	}
}

func (d *DNSService) CancelWorkers() {
	d.mxContexts.Lock()
	for _, ctx := range d.contexts {
		ctx.cancel()
	}
	d.contexts = make(map[data.UUID]*cancelContext)
	d.mxContexts.Unlock()
}

func (d *DNSService) SchedulerUpdateTrigger(ctx context.Context) error {
	nearTime, err := d.DNSRepo.GetNearTimeOfObsoleteRecord(ctx)
	if err != nil {
		return err
	}

	d.Scheduler.ScheduleHandler(nearTime)
	return nil
}

func (d *DNSService) updateDNSInformation(ctx context.Context, fqdns []string, operationStartTimePtr *time.Time) []error {
	dataSize := len(fqdns)

	// for 100,000 fqdns
	// Parallelize data processing.
	numCPU := runtime.GOMAXPROCS(0)

	var gorCountForData int
	if numCPU > dataSize {
		gorCountForData = dataSize
	} else {
		gorCountForData = numCPU
	}

	gorCountDNS := int(math.Floor(float64(gorCountForData) / 100.0 * 30.0))
	if gorCountDNS < 1 {
		gorCountDNS = 1
	}

	gorCountDB := int(math.Ceil(float64(gorCountForData) / 100.0 * 70.0))
	if gorCountDB < 1 {
		gorCountDB = 1
	}

	// Data chunks channel
	var cOut = make(chan []data.DNSInfo, gorCountDNS*2)
	errs := make([]error, 0, gorCountDNS+gorCountDB)
	mx := sync.Mutex{}
	var gorCompleted atomic.Int32

	processGorErrors := func(err error) {
		mx.Lock()
		errs = append(errs, err)
		mx.Unlock()
	}

	parallelize.Parallelize(
		gorCountDNS, fqdns, dataSize,
		func(gorIndex int, fqdnsChunk []string) {
			defer func() {
				if gorCompleted.Load() < int32(gorCountDNS-1) {
					gorCompleted.Add(1)
				} else {
					// All the goroutines have finished their work.
					close(cOut)
				}
			}()

			for _, fqdn := range fqdnsChunk {
				isCanceled, err := isContextCanceled(ctx)
				if err != nil {
					processGorErrors(err)
					return
				}

				if isCanceled {
					strOut := "updateDNSInformation(): Context canceled"
					logger.Info(strOut)
					return
				}

				// Obtain DNS information for fqdns from the specified DNS server.
				dnsInfo, err := d.dns.GetDNSInfo(d.config.DNSClient.DSN, fqdn)
				if err != nil {
					switch {
					case os.IsTimeout(err),
						errors.Is(err, os.ErrDeadlineExceeded):
						cOut <- nil
						continue
					}

					processGorErrors(err)
					return
				}

				// Nothing to update
				if len(dnsInfo) < 1 {
					logger.Warn("DNS server did not provide records for fqdn: " + fqdn)
					cOut <- nil
					continue
				}

				// Send data chunk
				cOut <- dnsInfo
			}
		})

	// Update new data in database.
	wg := sync.WaitGroup{}
	wg.Add(gorCountDB)
	parallelize.Parallelize(
		gorCountDB, fqdns, dataSize,
		func(gorIndex int, fqdnsChunk []string) {
			defer wg.Done()
		loop:
			for {
				select {
				case dnsInfo, ok := <-cOut:
					if !ok {
						break loop
					}

					if dnsInfo == nil {
						continue
					}

					// Convert DNSInfo data to model data format.
					dnsRecords, dnsRecordLifetime := d.convertDNSInfoToModel(dnsInfo)

					// Update DNS info in base.
					err := d.DNSRepo.DoTransaction(ctx, func(sctx context.Context) error {
						return d.DNSRepo.UpdateDNS(sctx, dnsRecords, dnsRecordLifetime, operationStartTimePtr)
					})
					if err != nil {
						logger.Error(fmt.Sprintf("updateDNSInformation()\n FAILED UpdateDNS count: %d dnsRecords: %v dnsRecordLifetime: %v\nerr: %s\n", len(dnsRecords), dnsRecords, dnsRecordLifetime, err))
						continue
					}

					// Update scheduler trigger time.
					err = d.SchedulerUpdateTrigger(ctx)
					if err != nil {
						logger.Error(fmt.Sprintf("updateDNSInformation()\n FAILED SchedulerUpdateTrigger(): %s", err))
						continue
					}
				}
			}
		})

	// Waiting for all background handlers to complete.
	wg.Wait()

	// Fix length.
	if len(errs) != cap(errs) {
		errsNew := make([]error, len(errs))
		copy(errsNew, errs)
		errs = errsNew
	}

	return errs
}

func (d *DNSService) ProcessDNSDataSchedulerCallback(ctx context.Context, operationStartTimePtr *time.Time, lastUpdateTimePtr *time.Time) error {
	// Scheduled update of data from the database
	// Get the data that needs to be updated from the database.
	fqdns, err := d.DNSRepo.GetDataRequiringUpdate(ctx, operationStartTimePtr, lastUpdateTimePtr)
	if err != nil {
		return err
	}

	// Nothing to update
	if len(fqdns) < 1 {
		logger.Warn("There is no DNS data in the database that needs to be updated!")
		return nil
	}

	errs := d.updateDNSInformation(ctx, fqdns, operationStartTimePtr)
	err = processErrors(errs)
	if err != nil {
		return err
	}

	return nil
}

func (d *DNSService) ProcessNewlyReceivedDNSData(fqdns []string) error {
	// Processing of data received from client RESP API
	ctx, cancel := context.WithCancel(context.Background())
	ctxID := uuid.GenerateUUID()
	d.mxContexts.Lock()
	d.contexts[ctxID] = &cancelContext{
		ctx,
		cancel,
	}
	d.mxContexts.Unlock()
	defer func() {
		d.mxContexts.Lock()
		delete(d.contexts, ctxID)
		d.mxContexts.Unlock()
	}()

	return d.processNewDNSData(ctx, fqdns)
}

// Function of processing just received data via REST API
func (d *DNSService) processNewDNSData(ctx context.Context, fqdns []string) error {
	// len(fqdns) about 10,000.
	// Start operation time.
	operationStartTimePtr := new(time.Time)
	*operationStartTimePtr = time.Now()

	fqdns = removeDuplicate(fqdns)
	logger.Info(fmt.Sprintf("Processing of newly acquired data.\n Number of FQDNs: %d\n", len(fqdns)))
	errs := d.updateDNSInformation(ctx, fqdns, operationStartTimePtr)
	err := processErrors(errs)
	if err != nil {
		return err
	}

	return nil
}

func (d *DNSService) convertDNSInfoToModel(dnsInfo []data.DNSInfo) ([]interface{}, []interface{}) {
	dnsRecords := make([]interface{}, len(dnsInfo))
	dnsRecordLifetime := make([]interface{}, len(dnsInfo))

	for index, info := range dnsInfo {
		// According to the task condition, the refresh rate is determined by the ttl dns record
		// TLL for "google.com." is 79 seconds
		// TLL for "qq.com." is 600 seconds
		// TTL can be equal to 0, i.e. a one-time DNS record.

		expiredAt := info.CreatedAt.Add(time.Second * time.Duration(info.TTL))
		dnsRecords[index] = domain.DNSRecords{
			A:    info.A,
			Fqdn: info.Fqdn,
		}

		dnsRecordLifetime[index] = domain.DNSRecordLifetime{
			CreatedAt: info.CreatedAt,
			ExpiredAt: expiredAt,
		}
	}

	return dnsRecords, dnsRecordLifetime
}

func (d *DNSService) GetDomainsByIP(ctx context.Context, ips []string) (data.IPDomainsData, error) {
	ips = removeDuplicate(ips)
	dataSize := len(ips)

	ipDomains := make(data.IPDomainsData, 0, dataSize)

	// ~3 sec for 10,000 ips
	// Parallelize data processing.
	numCPU := runtime.GOMAXPROCS(0)

	var gorCount int
	if numCPU > dataSize {
		gorCount = dataSize
	} else {
		gorCount = numCPU
	}

	// Data chunks channel
	var cOut = make(chan *data.IPDomains, gorCount)
	errs := make([]error, 0, gorCount)
	mx := sync.Mutex{}
	var gorCompleted atomic.Int32

	parallelize.Parallelize(
		gorCount, ips, dataSize,
		func(gorIndex int, ipsChunk []string) {
			defer func() {
				if gorCompleted.Load() < int32(gorCount-1) {
					gorCompleted.Add(1)
				} else {
					// All the goroutines have finished their work.
					close(cOut)
				}
			}()

			for _, ip := range ipsChunk {
				domains, err := d.DNSRepo.GetDomainsByIP(ctx, ip)
				if err != nil {
					mx.Lock()
					errs = append(errs, err)
					mx.Unlock()
					return
				}

				cOut <- &data.IPDomains{
					IP:      ip,
					Domains: domains,
				}
			}
		})

	// Process all data in the channel.
loop:
	for {
		select {
		case ipDomain, ok := <-cOut:
			if !ok {
				break loop
			}

			if len(ipDomain.Domains) < 1 {
				continue
			}

			ipDomains = append(ipDomains, *ipDomain)
		}
	}

	err := processErrors(errs)
	if err != nil {
		return nil, err
	}

	return ipDomains, nil
}

func processErrors(errs []error) error {
	if len(errs) != 0 {
		for _, err := range errs {
			logger.Error(err.Error())
		}

		return errs[0]
	}

	return nil
}

func isContextCanceled(ctx context.Context) (bool, error) {
	isCanceled, err := contextinf.IsContextCanceled(ctx)
	if err != nil {
		strOut := fmt.Sprintf("Context canceled error: %s", err)
		logger.Error(strOut)
	}

	return isCanceled, err
}

func removeDuplicate[T string | int](sliceList []T) []T {
	allKeys := make(map[T]bool)
	list := make([]T, 0, len(sliceList))
	for _, item := range sliceList {
		if _, value := allKeys[item]; !value {
			allKeys[item] = true
			list = append(list, item)
		}
	}
	return list
}
