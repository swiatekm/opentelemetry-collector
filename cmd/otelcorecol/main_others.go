// Code generated by "go.opentelemetry.io/collector/cmd/builder". DO NOT EDIT.

//go:build !windows
// +build !windows

package main

import "go.opentelemetry.io/collector/service"

func run(params service.CollectorSettings) error {
	return runInteractive(params)
}