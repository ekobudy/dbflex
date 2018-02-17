package test

import (
	"testing"
)

func TestClose(t *testing.T) {
	if conn != nil {
		conn.Close()
	}
}
