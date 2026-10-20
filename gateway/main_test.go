package main

import (
	"testing"
	"time"
)

func TestParseTopic(t *testing.T) {
	cases := []struct {
		topic, id, kind string
		ok              bool
	}{
		{"vigil/machines/m-0001/telemetry", "m-0001", "telemetry", true},
		{"vigil/machines/m-0001/status", "m-0001", "status", true},
		{"vigil/machines//telemetry", "", "", false},
		{"vigil/machines/m-0001", "", "", false},
		{"other/machines/m-0001/telemetry", "", "", false},
	}
	for _, c := range cases {
		id, kind, err := parseTopic(c.topic)
		if (err == nil) != c.ok || id != c.id || kind != c.kind {
			t.Errorf("parseTopic(%q) = %q, %q, %v", c.topic, id, kind, err)
		}
	}
}

func TestDeviceTimestamp(t *testing.T) {
	ts, ok := deviceTimestamp([]byte(`{"ts": 1700000000123}`))
	if !ok || !ts.Equal(time.UnixMilli(1700000000123)) {
		t.Fatalf("got %v %v", ts, ok)
	}
	if _, ok := deviceTimestamp([]byte(`not json`)); ok {
		t.Fatal("expected failure on invalid json")
	}
	if _, ok := deviceTimestamp([]byte(`{"seq": 1}`)); ok {
		t.Fatal("expected failure without ts")
	}
}
