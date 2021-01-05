// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2021 Snowplow Analytics Ltd. All rights reserved.

package retry

import (
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"math/rand"
	"time"
)

// Exponential provides the ability to exponentially retry the execution of a function
func Exponential(attempts int, sleep time.Duration, prefix string, f func() error) error {
	err := f()
	if err != nil {
		logrus.Warnf("Retrying func (attempts: %d): %s: %s", attempts, prefix, err)

		if attempts--; attempts > 0 {
			jitter := time.Duration(rand.Int63n(int64(sleep)))
			sleep = sleep + jitter/2
			time.Sleep(sleep)
			return Exponential(attempts, 2*sleep, prefix, f)
		}
		return errors.Wrap(err, prefix)
	}

	return nil
}

// ExponentialWithInterface provides the ability to exponentially retry the execution of a function
// and return a result from the function
func ExponentialWithInterface(attempts int, sleep time.Duration, prefix string, f func() (interface{}, error)) (interface{}, error) {
	res, err := f()
	if err != nil {
		logrus.Warnf("Retrying func (attempts: %d): %s: %s", attempts, prefix, err)

		if attempts--; attempts > 0 {
			jitter := time.Duration(rand.Int63n(int64(sleep)))
			sleep = sleep + jitter/2
			time.Sleep(sleep)
			return ExponentialWithInterface(attempts, 2*sleep, prefix, f)
		}
		return nil, errors.Wrap(err, prefix)
	}

	return res, nil
}
