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
	"testing"
)

const (
	simpleRegexp = "foo.*"
	complexRegexp = "foo([^.]{2})([so]{2}).{1}([^,])"
	lineMatch = "this is a very long line that contains foo, so that it will match"
	lineNoMatch = "this is a very long line that contains bar, so that it will not match"
	numGoroutines = 100
	poolSize = 10
)

func Benchmark_OriginalPattern_Simple_Match(b *testing.B) {
	regexToMatch, err := regexp.Compile(simpleRegexp)
	if err != nil {
		b.Fatal("failed to compile regex")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reMatch(regexToMatch, lineMatch, numGoroutines)
	}
}

func Benchmark_OriginalPattern_Simple_NoMatch(b *testing.B) {
	regexToMatch, err := regexp.Compile(simpleRegexp)
	if err != nil {
		b.Fatal("failed to compile regex")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reMatch(regexToMatch, lineNoMatch, numGoroutines)
	}
}


func Benchmark_CopyPattern_Simple_Match(b *testing.B) {
	regexToMatch, err := regexp.Compile(simpleRegexp)
	if err != nil {
		b.Fatal("failed to compile regex")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reMatch(regexToMatch.Copy(), lineMatch, numGoroutines)
	}
}

func Benchmark_CopyPattern_Simple_NoMatch(b *testing.B) {
	regexToMatch, err := regexp.Compile(simpleRegexp)
	if err != nil {
		b.Fatal("failed to compile regex")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reMatch(regexToMatch.Copy(), lineNoMatch, numGoroutines)
	}
}

func Benchmark_CyclePattern_Simple_Match(b *testing.B) {
	rePool := make([]*regexp.Regexp, poolSize)
	for i := 0; i < poolSize; i++ {
		regexToMatch, err := regexp.Compile(simpleRegexp)
		if err != nil {
			b.Fatal("failed to compile regex")
		}

		rePool[i] = regexToMatch
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reCycleMatch(rePool, lineMatch, numGoroutines)
	}
}

func Benchmark_CyclePattern_Simple_NoMatch(b *testing.B) {
	rePool := make([]*regexp.Regexp, poolSize)
	for i := 0; i < poolSize; i++ {
		regexToMatch, err := regexp.Compile(simpleRegexp)
		if err != nil {
			b.Fatal("failed to compile regex")
		}

		rePool[i] = regexToMatch
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reCycleMatch(rePool, lineNoMatch, numGoroutines)
	}
}

func Benchmark_OriginalPattern_Complex_Match(b *testing.B) {
	regexToMatch, err := regexp.Compile(complexRegexp)
	if err != nil {
		b.Fatal("failed to compile regex")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reMatch(regexToMatch, lineMatch, numGoroutines)
	}
}

func Benchmark_OriginalPattern_Complex_NoMatch(b *testing.B) {
	regexToMatch, err := regexp.Compile(complexRegexp)
	if err != nil {
		b.Fatal("failed to compile regex")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reMatch(regexToMatch, lineNoMatch, numGoroutines)
	}
}


func Benchmark_CopyPattern_Complex_Match(b *testing.B) {
	regexToMatch, err := regexp.Compile(complexRegexp)
	if err != nil {
		b.Fatal("failed to compile regex")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reMatch(regexToMatch.Copy(), lineMatch, numGoroutines)
	}
}

func Benchmark_CopyPattern_Complex_NoMatch(b *testing.B) {
	regexToMatch, err := regexp.Compile(complexRegexp)
	if err != nil {
		b.Fatal("failed to compile regex")
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reMatch(regexToMatch.Copy(), lineNoMatch, numGoroutines)
	}
}

func Benchmark_CyclePattern_Complex_Match(b *testing.B) {
	rePool := make([]*regexp.Regexp, poolSize)
	for i := 0; i < poolSize; i++ {
		regexToMatch, err := regexp.Compile(complexRegexp)
		if err != nil {
			b.Fatal("failed to compile regex")
		}

		rePool[i] = regexToMatch
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reCycleMatch(rePool, lineMatch, numGoroutines)
	}
}

func Benchmark_CyclePattern_Complex_NoMatch(b *testing.B) {
	rePool := make([]*regexp.Regexp, poolSize)
	for i := 0; i < poolSize; i++ {
		regexToMatch, err := regexp.Compile(complexRegexp)
		if err != nil {
			b.Fatal("failed to compile regex")
		}

		rePool[i] = regexToMatch
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		reCycleMatch(rePool, lineNoMatch, numGoroutines)
	}
}


// Results
// $ go test -bench . -benchtime 20s
// goos: linux
// goarch: amd64
// pkg: github.com/snowplow/snowbridge/regex_test
// Benchmark_OriginalPattern_Simple_Match-12       	  151987	    163473 ns/op	    5191 B/op	     101 allocs/op
// Benchmark_OriginalPattern_Simple_NoMatch-12     	  147091	    162342 ns/op	    5084 B/op	     101 allocs/op
// Benchmark_CopyPattern_Simple_Match-12           	  151992	    160902 ns/op	    5370 B/op	     102 allocs/op
// Benchmark_CopyPattern_Simple_NoMatch-12         	  146196	    162909 ns/op	    5276 B/op	     102 allocs/op
// Benchmark_CyclePattern_Simple_Match-12          	  142602	    162589 ns/op	    6910 B/op	     101 allocs/op
// Benchmark_CyclePattern_Simple_NoMatch-12        	  148164	    163421 ns/op	    6752 B/op	     101 allocs/op
// Benchmark_OriginalPattern_Complex_Match-12      	  147717	    161511 ns/op	    5137 B/op	     101 allocs/op
// Benchmark_OriginalPattern_Complex_NoMatch-12    	  163436	    164527 ns/op	    5079 B/op	     101 allocs/op
// Benchmark_CopyPattern_Complex_Match-12          	  142876	    160215 ns/op	    5301 B/op	     102 allocs/op
// Benchmark_CopyPattern_Complex_NoMatch-12        	  165584	    161044 ns/op	    5250 B/op	     102 allocs/op
// Benchmark_CyclePattern_Complex_Match-12         	  146602	    163774 ns/op	    6818 B/op	     101 allocs/op
// Benchmark_CyclePattern_Complex_NoMatch-12       	  151538	    158917 ns/op	    6714 B/op	     101 allocs/op
