package test

import (
	"testing"
)

func TestClose(t *testing.T) {
	if sess != nil {
		sess.Close()
	}
	if conn != nil {
		conn.Close()
	}
}
