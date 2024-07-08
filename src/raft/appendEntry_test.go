package raft

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBitMapAdd(t *testing.T) {
	tests := []struct {
		name     string
		input    bitMap
		arg      int
		expected bitMap
	}{
		{
			name:     "Add to empty bit map",
			input:    0,
			arg:      0,
			expected: 1,
		},
		{
			name:     "Add to non-empty bit map",
			input:    21,
			arg:      2,
			expected: 21,
		},
		{
			name:     "Add to max bit map",
			input:    ^bitMap(0),
			arg:      0,
			expected: ^bitMap(0),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.input.add(tt.arg)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBitMapLen(t *testing.T) {
	tests := []struct {
		name     string
		input    bitMap
		expected int
	}{
		{
			name:     "Empty bit map",
			input:    0,
			expected: 0,
		},
		{
			name:     "Non-empty bit map",
			input:    21,
			expected: 3,
		},
		{
			name:     "Max bit map",
			input:    ^bitMap(0),
			expected: 64,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.input.len()
			assert.Equal(t, tt.expected, result)
		})
	}
}
