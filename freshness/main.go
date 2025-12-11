package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type SiteFresh struct {
	Site            string  `json:"site"`
	LatestTimestamp float64 `json:"latest_timestamp"`
	AgeSeconds      float64 `json:"age_seconds"`
}

type FreshnessResp struct {
	Sites []SiteFresh `json:"sites"`
}

var (
	apiBaseURL = envOr("API_BASE_URL", "http://dtms-api:8003")
	port       = envOr("PORT", "8004")
	interval   = envOrInt("POLL_INTERVAL_SECONDS", 30)
	threshold  = envOrInt("FRESHNESS_THRESHOLD_SECONDS", 300)
)

var (
	gaugeFreshSeconds = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dtms_data_fresh_seconds",
			Help: "Age in seconds since last transfer for a site",
		},
		[]string{"site"},
	)
	gaugeFreshOk = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "dtms_data_fresh_ok",
			Help: "1 if freshness is below threshold, 0 otherwise",
		},
		[]string{"site"},
	)
	client = &http.Client{
		Timeout: 10 * time.Second,
	}
)

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envOrInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		var t int
		_, err := fmt.Sscanf(v, "%d", &t)
		if err == nil {
			return t
		}
	}
	return def
}

func init() {
	prometheus.MustRegister(gaugeFreshSeconds)
	prometheus.MustRegister(gaugeFreshOk)
}

func fetchFreshness(ctx context.Context) (*FreshnessResp, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", apiBaseURL+"/freshness", nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	var f FreshnessResp
	if err := json.Unmarshal(body, &f); err != nil {
		return nil, fmt.Errorf("json unmarshal: %w / body: %s", err, string(body))
	}
	return &f, nil
}

func pollLoop(ctx context.Context) {
	t := time.NewTicker(time.Duration(interval) * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			f, err := fetchFreshness(ctx)
			if err != nil {
				fmt.Printf("[freshness] fetch error: %v\n", err)
				continue
			}
			now := time.Now()
			for _, s := range f.Sites {
				gaugeFreshSeconds.WithLabelValues(s.Site).Set(s.AgeSeconds)
				ok := 0.0
				if s.AgeSeconds <= float64(threshold) {
					ok = 1.0
				}
				gaugeFreshOk.WithLabelValues(s.Site).Set(ok)
				fmt.Printf("[freshness] site=%s age=%.2fs ok=%v\n", s.Site, s.AgeSeconds, ok == 1.0)
			}
			_ = now
		}
	}
}

func main() {
	// allow override via flags
	flag.Parse()

	http.Handle("/metrics", promhttp.Handler())
	srv := &http.Server{
		Addr: ":" + port,
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go pollLoop(ctx)

	fmt.Printf("[freshness] starting on :%s polling %s every %ds\n", port, apiBaseURL, interval)
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		fmt.Printf("server error: %v\n", err)
	}
}
