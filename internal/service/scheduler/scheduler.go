package scheduler

import (
	"context"
	"errors"
	"fmt"
	"reversedns/internal/infrastructure/contextinf"
	"reversedns/internal/infrastructure/uuid"
	"reversedns/internal/service/data"
	"reversedns/pkg/logger"
	"sync"
	"time"
)

var (
	ErrContextCanceled = errors.New("context canceled")
	ErrUpdateTrigger   = errors.New("updating the handler trigger")
)

type Scheduler struct {
	muStartHandler            sync.Mutex
	handler                   func(ctx context.Context, operationStartTimePtr *time.Time, lastUpdateTimePtr *time.Time) error
	postHandler               func(ctx context.Context) error
	contexts                  map[data.UUID]*cancelContext
	mxContexts                sync.Mutex
	lastUpdateTimePtr         *time.Time
	numberOfScheduledHandlers int
}

type cancelContext struct {
	isInProgress bool
	ctx          context.Context
	cancel       context.CancelCauseFunc
	atTime       *time.Time
}

func NewScheduler() *Scheduler {
	return &Scheduler{
		contexts:          make(map[data.UUID]*cancelContext),
		lastUpdateTimePtr: &time.Time{},
	}
}

func (s *Scheduler) ScheduleHandler(atTime *time.Time) {
	go func(atTime *time.Time) {
		ctx, cancel := context.WithCancelCause(context.Background())
		ctxID := uuid.GenerateUUID()
		cctx := &cancelContext{
			ctx:    ctx,
			cancel: cancel,
			atTime: atTime,
		}

		s.mxContexts.Lock()
		for key, cctxCur := range s.contexts {
			// Do not cancel the trigger if it occurs early in the timeline.
			cmp := cctxCur.atTime.Compare(*atTime)
			switch {
			case cmp == -1:
			// cctxCur.atTime is before atTime
			// Perhaps the REST request passed data that contained the FQDN
			// for a record already in the database. Therefore, its lifetime
			// has been updated and the old scheduled handler is no longer relevant.
			// So we cancel it.
			case cmp == 0:
				// cctxCur.atTime is equal atTime
				// Cancel scheduling of the new handler.
				//logger.Info(fmt.Sprintf("There is no need to schedule a new handler. %v == %v\n", *cctxCur.atTime, *atTime))
				s.mxContexts.Unlock()
				return
			// case cmp == 1:
			//	// cctxCur.atTime is after atTime
			// Perhaps the REST request passed data that contained new FQDNs
			// that are not yet in the database. These records expire much earlier
			// than the records received before, and the old scheduled handler is no longer relevant.
			// So we cancel it.
			default:
			}

			//
			// if cctxCur.atTime.Before(*atTime) {
			//	continue
			//}

			s.muStartHandler.Lock()
			if !cctxCur.isInProgress {
				// Cancel and delete old scheduled handlers
				// if they exist and if they are not executed.

				cctxCur.cancel(ErrUpdateTrigger)
				delete(s.contexts, key)
				s.numberOfScheduledHandlers--
				logger.Info(fmt.Sprintf("Scheduled handler canceled %v in favor of %v\n", *cctxCur.atTime, *atTime))
			}
			s.muStartHandler.Unlock()
		}

		// Save new handler
		s.contexts[ctxID] = cctx
		s.numberOfScheduledHandlers++
		s.mxContexts.Unlock()
		defer func() {
			s.mxContexts.Lock()
			delete(s.contexts, ctxID)
			s.numberOfScheduledHandlers--
			s.mxContexts.Unlock()
		}()

		err := s.scheduleProcessor(cctx, atTime)
		if err != nil {
			switch {
			case errors.Is(err, ErrUpdateTrigger),
				errors.Is(err, ErrContextCanceled):
				logger.Warn("ScheduleHandler " + err.Error())
				return
			default:
				logger.Error("ScheduleHandler " + err.Error())
				return
			}
		}

		err = s.postHandler(ctx)
		if err != nil {
			logger.Warn("ScheduleHandler " + err.Error())
			return
		}
	}(atTime)
}

func (s *Scheduler) scheduleProcessor(cctx *cancelContext, atTime *time.Time) error {
	logger.Info(fmt.Sprintf("The scheduler has planned the next DNS data update for %v\n", atTime))

	timer := time.NewTimer(time.Until(*atTime))
	defer timer.Stop()

	select {
	case <-timer.C:
		// Mark that the scheduled handler has started its processing.
		s.muStartHandler.Lock()
		cctx.isInProgress = true

		// Check if the handler context has been canceled before.
		// If the context has been canceled, cancel the handler.
		isCanceled, err := contextinf.IsContextCanceled(cctx.ctx)
		if err != nil {
			logger.Error(fmt.Sprintf("Scheduler context canceled error: %s", err))
			cctx.cancel(err)
		}

		if isCanceled {
			s.muStartHandler.Unlock()
			return context.Cause(cctx.ctx)
		}

		lastUpdateTimePtr := s.lastUpdateTimePtr

		operationStartTimePtr := new(time.Time)
		*operationStartTimePtr = time.Now()

		s.lastUpdateTimePtr = operationStartTimePtr

		s.muStartHandler.Unlock()

		// Call handler
		err = s.handler(cctx.ctx, operationStartTimePtr, lastUpdateTimePtr)
		if err != nil {
			logger.Error("ScheduleHandler error:" + err.Error())
			return err
		}
	case <-cctx.ctx.Done():
		// Canceled
		return context.Cause(cctx.ctx)
	}

	return nil
}

func (s *Scheduler) SetHandler(handler func(ctx context.Context, operationStartTimePtr *time.Time, lastUpdateTimePtr *time.Time) error) {
	s.handler = handler
}

func (s *Scheduler) SetPostHandler(postHandler func(ctx context.Context) error) {
	s.postHandler = postHandler
}
