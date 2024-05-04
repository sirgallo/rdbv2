package utils

import "errors"
import "math/rand"
import "time"


//============================================= Exponential Backoff Utils


// ExpBackoffOpts
//	Struct to initialize the exponential backoff
type ExpBackoffOpts struct {
	// TimeoutInMilliseconds: the initial timeout
	TimeoutInNanosecs int
	// MaxRetries: the total amount of retries
	MaxRetries *int // optional field, use a pointer
}

// ExponentialBackoffStrat
//	The exponential backoff strategy
type ExponentialBackoffStrat [T comparable] struct {
	// depth: the current retry
	depth int
	// initialTimeout: the initial timeout period for the first retry
	initialTimeout int
	// currentTimeout: the timeout for the current depth
	currentTimeout int
	// maxRetries: the total retries for the strat. Can be infinite if nil
	maxRetries *int
}


// DefaultMaxRetries
//	Let's use this to represent unlimited retries
const DefaultMaxRetries = -1


// NewExponentialBackoffStrat
//	Create a new exponential backoff strategy with passable options.
//
// Parameters:
//	opts: options to initialize the exponential backoff strat
//
// Returns:
//	Initialized exponential backoff strategy
func NewExponentialBackoffStrat [T comparable](opts ExpBackoffOpts) *ExponentialBackoffStrat[T] {
	maxRetries := DefaultMaxRetries
	if opts.MaxRetries != nil { maxRetries = *opts.MaxRetries }

	return &ExponentialBackoffStrat[T]{
		depth: 1, 
		initialTimeout: opts.TimeoutInNanosecs,
		currentTimeout: opts.TimeoutInNanosecs,
		maxRetries: &maxRetries,
	}
}

// PerformBackoff
//	Pass in a function that returns type T, which will be our operation
//	If success:
//		Return the response
//	If error:
//		1.) Sleep for the current timeout period
//		2.) Recalculate the timeout for the next backoff period using:
//			New timeout = 2 ^ (depth - 1) * current timeout
//		3.) Step to next retry
//	If current depth exceeds the max retries defined:
//		Return max retries error to indicate the operation failed
//
// Parameters:
//	operation: the operation to be performed where if it fails, retry
//
// Returns:
//	Either a response after successful retries, or an error if max retries have been met
func (expStrat *ExponentialBackoffStrat[T]) PerformBackoff(operation func() (T, error)) (T, error) {
	for {
		if expStrat.depth > *expStrat.maxRetries && *expStrat.maxRetries != DefaultMaxRetries { 
			return GetZero[T](), errors.New("process reached max retries on exponential backoff") 
		}

		res, err := operation()
		if err != nil {
			jitter := expStrat.generateJitter()
			time.Sleep(time.Duration(expStrat.currentTimeout + jitter) * time.Nanosecond)
			
			expStrat.currentTimeout = expStrat.currentTimeout << (expStrat.depth - 1)
			expStrat.depth++
		} else { return res, nil }
	}
}

// Reset
//	Reset the exponential backoff to highest depth and initial timeout
func (expStrat *ExponentialBackoffStrat[T]) Reset() {
	expStrat.depth = 1
	expStrat.currentTimeout = expStrat.initialTimeout
}

// generateJitter
//	Add randomness to the timeout period, which is either +/- value in range of 25% of the current timeout.
//
// Returns:
//	The random value for the jitter
func (expStrat *ExponentialBackoffStrat[T]) generateJitter() int {
	n := expStrat.currentTimeout / 4
	return rand.Intn(2 * n + 1) - n
}