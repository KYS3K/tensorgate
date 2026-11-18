package admission

import (
	"math"
	"math/rand/v2"
	"strconv"
	"time"
)

const maxRetryAfter = 60 * time.Second
const minSeconds = 1

func RetryAfter(d, jitter time.Duration) string {
	d = min(d, maxRetryAfter)
	if jitter > 0 {
		d += time.Duration(rand.Int64N(int64(jitter)))
	}
	seconds := int(math.Ceil(d.Seconds()))
	seconds = max(seconds, minSeconds)
	return strconv.Itoa(seconds)
}
