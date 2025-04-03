package common

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

var (
	// --- General ---
	ErrorsTotal = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3runfast_errors_total",
			Help: "Total number of errors encountered, labeled by component and type.",
		},
		[]string{"component", "type"},
	)

	// --- Queue ---
	TasksPushed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3runfast_tasks_pushed_total",
			Help: "Total number of tasks pushed to queues.",
		},
		[]string{"queue"},
	)
	TasksPopped = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3runfast_tasks_popped_total",
			Help: "Total number of tasks popped from queues.",
		},
		[]string{"queue"},
	)
	EventsPublished = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3runfast_events_published_total",
			Help: "Total number of events published.",
		},
		[]string{"topic"},
	)

	// --- Worker ---
	TasksProcessed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "s3runfast_tasks_processed_total",
			Help: "Total number of tasks processed by workers.",
		},
		[]string{"worker_type", "status"}, // status: completed, failed, skipped
	)
	ObjectsMigratedBytes = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "s3runfast_objects_migrated_bytes_total",
			Help: "Total bytes of objects successfully migrated.",
		},
	)

	// --- Controller ---
	BucketsScanned = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "s3runfast_buckets_scanned_total",
			Help: "Total number of source buckets scanned.",
		},
	)
	ObjectsListed = promauto.NewCounter(
		prometheus.CounterOpts{
			Name: "s3runfast_objects_listed_total",
			Help: "Total number of objects listed from source buckets.",
		},
	)
)

// StartMetricsServer starts an HTTP server to expose Prometheus metrics.
func StartMetricsServer(addr string) {
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Infof("Starting metrics server on %s", addr)
		if err := http.ListenAndServe(addr, nil); err != nil {
			log.Errorf("Metrics server failed: %v", err)
		}
	}()
}
