/**
 * Copyright (c) 2020-present Snowplow Analytics Ltd.
 * All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd.,
 * under the terms of the Snowplow Limited Use License Agreement, Version 1.0
 * located at https://docs.snowplow.io/limited-use-license-1.0
 * BY INSTALLING, DOWNLOADING, ACCESSING, USING OR DISTRIBUTING ANY PORTION
 * OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */

package regexpatterns

import (
	"regexp"
)


func reMatch(re *regexp.Regexp, line string, numGoroutines int) {
	c := make(chan int)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			re.MatchString(line)
			c <- 1
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		<-c
	}
}

func reCycleMatch(rePool []*regexp.Regexp, line string, numGoroutines int) {
	c := make(chan int)
	for i := 0; i < numGoroutines; i++ {
		go func() {
			rePool[(i % poolSize)].MatchString(line)
			c <- 1
		}()
	}

	for i := 0; i < numGoroutines; i++ {
		<-c
	}
}
