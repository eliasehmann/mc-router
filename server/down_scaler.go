package server

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// TODO need to re-evaluate the awkwardness of DownScaler in the overall whole flow.
// Maybe it needs to be renamed too, such as  DownScalingTimers

type IDownScaler interface {
	Reset()
	Start(ctx context.Context, scalingTarget ScalingTarget, routes IRoutes)
	// UseDelay overrides the scale-down delay for the given scaling target. A delay of zero
	// removes the override, so the target goes back to the default delay. A timer that is
	// already running keeps the delay it was started with.
	UseDelay(scalingTarget ScalingTarget, delay time.Duration)
	Cancel(scalingTarget ScalingTarget)
	HandleContextDone(ctx context.Context)
}

func NewDownScaler(enabled bool, delay time.Duration) IDownScaler {
	return &downScalerImpl{
		enabled: enabled,
		delay:   delay,
		delays:  make(map[string]time.Duration),
		timers:  make(map[string]*time.Timer),
	}
}

type downScalerImpl struct {
	sync.Mutex
	enabled bool
	delay   time.Duration
	// delays holds the per-target overrides of delay, keyed like timers
	delays map[string]time.Duration
	timers map[string]*time.Timer
}

func (ds *downScalerImpl) Reset() {
	ds.Lock()
	defer ds.Unlock()

	for _, t := range ds.timers {
		t.Stop()
	}
	ds.timers = make(map[string]*time.Timer)
	ds.delays = make(map[string]time.Duration)
}

func (ds *downScalerImpl) UseDelay(scalingTarget ScalingTarget, delay time.Duration) {
	ds.Lock()
	defer ds.Unlock()

	if delay > 0 {
		ds.delays[scalingTarget.ScalingKey()] = delay
	} else {
		delete(ds.delays, scalingTarget.ScalingKey())
	}
}

func (ds *downScalerImpl) Start(ctx context.Context, scalingTarget ScalingTarget, routes IRoutes) {
	ds.Lock()
	defer ds.Unlock()

	if !ds.enabled {
		return
	}

	key := scalingTarget.ScalingKey()
	if _, exists := ds.timers[key]; exists {
		// Already scheduled; prevent duplicate scale-down for same target
		return
	}

	delay, overridden := ds.delays[key]
	if !overridden {
		delay = ds.delay
	}

	logrus.WithField("scalingTarget", scalingTarget).
		WithField("delay", delay).
		Debug("Starting scale-down timer")

	ds.timers[key] = time.AfterFunc(delay, func() {
		ds.Lock()
		delete(ds.timers, key)
		ds.Unlock()

		select {
		case <-ctx.Done():
			return
		default:
		}

		sleeper := routes.GetSleeper(scalingTarget)
		logrus.WithField("scalingTarget", scalingTarget).
			WithField("found", sleeper != nil).
			Debug("Looking for sleeper to use")
		if sleeper == nil {
			return
		}

		if scalingTarget.StartScaling() {
			defer scalingTarget.EndScaling()

			logrus.WithField("scalingTarget", scalingTarget).Info("Scaling down backend server")
			if err := sleeper(ctx); err != nil {
				logrus.WithError(err).
					WithField("scalingTarget", scalingTarget).
					Error("Error while executing sleeper function")
			}
		}
	})
}

func (ds *downScalerImpl) Cancel(scalingTarget ScalingTarget) {
	ds.Lock()
	defer ds.Unlock()

	if !ds.enabled {
		return
	}

	key := scalingTarget.ScalingKey()
	if t, ok := ds.timers[key]; ok {
		logrus.WithField("scalingTarget", scalingTarget).Debug("Canceling scale-down timer")
		t.Stop()
		delete(ds.timers, key)
	}
}

func (ds *downScalerImpl) HandleContextDone(ctx context.Context) {
	go func() {
		<-ctx.Done()
		ds.Reset()
	}()
}
