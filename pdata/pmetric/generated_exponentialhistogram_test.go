// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Code generated by "pdata/internal/cmd/pdatagen/main.go". DO NOT EDIT.
// To regenerate this file run "make genpdata".

package pmetric

import (
	"testing"

	"github.com/stretchr/testify/assert"

	otlpmetrics "go.opentelemetry.io/collector/pdata/internal/data/protogen/metrics/v1"
)

func TestExponentialHistogram_MoveTo(t *testing.T) {
	ms := generateTestExponentialHistogram()
	dest := NewExponentialHistogram()
	ms.MoveTo(dest)
	assert.Equal(t, NewExponentialHistogram(), ms)
	assert.Equal(t, generateTestExponentialHistogram(), dest)
}

func TestExponentialHistogram_CopyTo(t *testing.T) {
	ms := NewExponentialHistogram()
	orig := NewExponentialHistogram()
	orig.CopyTo(ms)
	assert.Equal(t, orig, ms)
	orig = generateTestExponentialHistogram()
	orig.CopyTo(ms)
	assert.Equal(t, orig, ms)
}

func TestExponentialHistogram_AggregationTemporality(t *testing.T) {
	ms := NewExponentialHistogram()
	assert.Equal(t, AggregationTemporality(otlpmetrics.AggregationTemporality(0)), ms.AggregationTemporality())
	testValAggregationTemporality := AggregationTemporality(otlpmetrics.AggregationTemporality(1))
	ms.SetAggregationTemporality(testValAggregationTemporality)
	assert.Equal(t, testValAggregationTemporality, ms.AggregationTemporality())
}

func TestExponentialHistogram_DataPoints(t *testing.T) {
	ms := NewExponentialHistogram()
	assert.Equal(t, NewExponentialHistogramDataPointSlice(), ms.DataPoints())
	fillTestExponentialHistogramDataPointSlice(ms.DataPoints())
	assert.Equal(t, generateTestExponentialHistogramDataPointSlice(), ms.DataPoints())
}

func generateTestExponentialHistogram() ExponentialHistogram {
	tv := NewExponentialHistogram()
	fillTestExponentialHistogram(tv)
	return tv
}

func fillTestExponentialHistogram(tv ExponentialHistogram) {
	tv.orig.AggregationTemporality = otlpmetrics.AggregationTemporality(1)
	fillTestExponentialHistogramDataPointSlice(newExponentialHistogramDataPointSlice(&tv.orig.DataPoints))
}