// PROPRIETARY AND CONFIDENTIAL
//
// Unauthorized copying of this file via any medium is strictly prohibited.
//
// Copyright (c) 2020-2022 Snowplow Analytics Ltd. All rights reserved.

package config

import (
	"encoding/json"
	"errors"

	"github.com/caarlos0/env/v6"
	"github.com/go-akka/configuration"
	"github.com/go-akka/configuration/hocon"
)

// EnvDecode populates target from the environment.
// The target argument must be a pointer to a struct type value.
func EnvDecode(target interface{}) error {
	return env.Parse(target)
}

// HoconDecode decodes HOCON source into a target interface.
// The target argument must be a pointer to a struct type value.
func HoconDecode(src []byte, target interface{}) (err error) {
	// Defering recover in case of panic.
	// reason: https://github.com/go-akka/configuration/issues/9
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("unknown panic recovered from decoding hocon")
			}
		}
	}()

	// Parsing HOCON
	hoconRoot := configuration.ParseString(string(src)).Root()
	// Preparing a map from parsed HOCON
	configTree := visitNode(hoconRoot)

	// Encoding the map in JSON and json-decoding into the target Go struct
	// See also:
	// https://github.com/elastic/harp/blob/main/pkg/template/values/hocon/hocon.go
	// https://github.com/open-policy-agent/conftest/blob/master/parser/hocon/hocon.go
	var jsonSrc []byte
	jsonSrc, err = json.Marshal(configTree)
	if err != nil {
		return
	}

	err = json.Unmarshal(jsonSrc, target)
	return
}

func visitNode(node *hocon.HoconValue) interface{} {
	if node.IsArray() {
		nodes := node.GetArray()

		res := make([]interface{}, len(nodes))
		for i, n := range nodes {
			res[i] = visitNode(n)
		}

		return res
	}

	if node.IsObject() {
		obj := node.GetObject()

		res := map[string]interface{}{}
		keys := obj.GetKeys()
		for _, k := range keys {
			res[k] = visitNode(obj.GetKey(k))
		}

		return res
	}

	if node.IsString() {
		return node.GetString()
	}

	if node.IsEmpty() {
		return nil
	}

	return nil
}
