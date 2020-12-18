// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020 Snowplow Analytics Ltd. All rights reserved.

package main

import (
	"fmt"
	"strings"
)

// EnumValue defines a structure for holding a CLI enum value set
type EnumValue struct {
	Enum     []string
	Default  string
	selected string
}

// Set checks and sets the value to be used
func (e *EnumValue) Set(value string) error {
	for _, enum := range e.Enum {
		if enum == value {
			e.selected = value
			return nil
		}
	}

	return fmt.Errorf("allowed values are %s", strings.Join(e.Enum, ", "))
}

// String returns the selected value
func (e EnumValue) String() string {
	if e.selected == "" {
		return e.Default
	}
	return e.selected
}
