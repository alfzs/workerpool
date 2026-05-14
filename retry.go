package workerpool

import (
	"math/rand/v2"
	"time"
)

type RetryPredicate func(error) bool

func DefaultRetryPredicate(err error) bool {
	return true
}

func calculateBackoff(
	rng *rand.Rand,
	attempt int,
	minDelay time.Duration,
	maxDelay time.Duration,
) time.Duration {

	shift := attempt - 1
	if shift > 30 {
		shift = 30
	}

	delay := minDelay * time.Duration(1<<uint(shift))

	if delay > maxDelay {
		delay = maxDelay
	}

	jitter := time.Duration(float64(delay) * 0.25)

	delay =
		delay -
			jitter +
			time.Duration(rng.Int64N(int64(jitter*2)))

	return delay
}
