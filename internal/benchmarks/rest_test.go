package benchmarks

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/x-name15/tinymq/internal/broker"
	"github.com/x-name15/tinymq/internal/transport/rest"
)

func BenchmarkRESTPublish(b *testing.B) {
	brk := broker.New(nil)
	svr := rest.NewServer(brk, "0", "bench", nil)
	ts := httptest.NewServer(svr.Handler())
	defer ts.Close()

	topic := "bench"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte(`{"hello":"world"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		url := ts.URL + "/publish/" + topic
		resp, err := http.Post(url, "application/json", bytes.NewReader(payload))
		if err != nil {
			b.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()
	}
}

func BenchmarkRESTConsume(b *testing.B) {
	brk := broker.New(nil)
	svr := rest.NewServer(brk, "0", "bench", nil)
	ts := httptest.NewServer(svr.Handler())
	defer ts.Close()

	topic := "bench"
	brk.CreateTopic(topic, "reject", 0)
	payload := []byte(`{"hello":"world"}`)

	for i := 0; i < 100; i++ {
		_ = brk.Publish(topic, payload, nil, "normal", nil, nil, false)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		url := ts.URL + "/consume/" + topic + "?auto_ack=true&timeout=0s"
		resp, err := http.Get(url)
		if err != nil {
			b.Fatalf("request failed: %v", err)
		}
		resp.Body.Close()
	}
}
