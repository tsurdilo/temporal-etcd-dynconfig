package etcddynconfig

import "sync/atomic"

// atomicValue is a typesafe wrapper over sync/atomic.Value.
type atomicValue[T any] struct {
	atomic.Value
}

func (v *atomicValue[T]) Store(x T) {
	v.Value.Store(x)
}

// Load returns the stored value and whether one was present.
func (v *atomicValue[T]) Load() (T, bool) {
	x := v.Value.Load()
	if x == nil {
		var zero T
		return zero, false
	}
	return x.(T), true //nolint:forcetypeassert
}
