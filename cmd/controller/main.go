package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"S3RunFast/pkg/common"

	"S3RunFast/pkg/queue"
	"S3RunFast/pkg/storage"
	"S3RunFast/pkg/workflow"

	log "github.com/sirupsen/logrus"
)

type Controller struct {
	config   *common.Config
	sourceS3 *storage.S3Client
	destS3   *storage.S3Client
	queue    *queue.RedisQueue
	ctx      context.Context
	cancel   context.CancelFunc
}

func NewController(config *common.Config) (*Controller, error) {
	// 创建源S3客户端
	sourceS3, err := storage.NewS3Client(&config.Source)
	if err != nil {
		return nil, fmt.Errorf("failed to create source S3 client: %v", err)
	}

	// 创建目标S3客户端
	destS3, err := storage.NewS3Client(&config.Destination)
	if err != nil {
		return nil, fmt.Errorf("failed to create destination S3 client: %v", err)
	}

	// 创建Redis队列
	redisQueue, err := queue.NewRedisQueue(&config.Queue)
	if err != nil {
		return nil, fmt.Errorf("failed to create Redis queue: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Controller{
		config:   config,
		sourceS3: sourceS3,
		destS3:   destS3,
		queue:    redisQueue,
		ctx:      ctx,
		cancel:   cancel,
	}, nil
}

func (c *Controller) Start() error {
	// 1. 启动桶创建事件监听
	go c.listenBucketCreationEvents()

	// 2. 启动源桶扫描
	go c.scanSourceBuckets()

	// 3. 启动 Metrics Server (if configured)
	if c.config.System.MetricsAddr != "" {
		common.StartMetricsServer(c.config.System.MetricsAddr)
	}

	// 4. 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// 5. 优雅关闭
	log.Info("Shutting down controller...")
	c.Shutdown()
	return nil
}

func (c *Controller) Shutdown() {
	c.cancel()
	c.queue.Close()
}

// scanSourceBuckets 扫描源S3中的所有桶
func (c *Controller) scanSourceBuckets() {
	log.Info("Starting source bucket scan...")
	buckets, err := c.sourceS3.ListBuckets()
	if err != nil {
		log.Errorf("Failed to list source buckets: %v", err)
		common.ErrorsTotal.WithLabelValues("controller", "list_buckets").Inc()
		return
	}
	log.Infof("Found %d source buckets", len(buckets))

	for _, bucket := range buckets {
		select {
		case <-c.ctx.Done():
			log.Info("Bucket scan cancelled.")
			return
		default:
			common.BucketsScanned.Inc()
			// 创建桶创建任务
			task := workflow.NewCreateBucketTask(bucket)
			if err := c.queue.PushTask(c.config.Queue.BucketCreationQueue, task); err != nil {
				log.Errorf("Failed to push bucket creation task for %s: %v", bucket, err)
				common.ErrorsTotal.WithLabelValues("controller", "push_task").Inc()
				continue
			}
			common.TasksPushed.WithLabelValues(c.config.Queue.BucketCreationQueue).Inc()
			log.Infof("Pushed bucket creation task for %s", bucket)
		}
	}
	log.Info("Finished source bucket scan.")
}

// listenBucketCreationEvents 监听桶创建完成事件
func (c *Controller) listenBucketCreationEvents() {
	log.Info("Starting to listen for bucket creation events")
	handler := func(data []byte) error {
		var event struct {
			Bucket string `json:"bucket"`
			Status string `json:"status"`
		}
		if err := json.Unmarshal(data, &event); err != nil {
			common.ErrorsTotal.WithLabelValues("controller", "unmarshal_event").Inc()
			return fmt.Errorf("failed to unmarshal event: %v", err)
		}

		if event.Status == "created" {
			// 开始扫描该桶中的对象
			go c.scanBucketObjects(event.Bucket)
		}

		return nil
	}

	log.Info("Stopped listening for bucket creation events")
	if err := c.queue.SubscribeEvent(c.config.Queue.BucketCreationTopic, handler); err != nil {
		log.Errorf("Failed to subscribe to bucket creation events: %v", err)
		common.ErrorsTotal.WithLabelValues("controller", "subscribe_events").Inc()
	}
}

// scanBucketObjects 扫描桶中的所有对象
func (c *Controller) scanBucketObjects(bucket string) {
	log.Infof("Scanning objects in bucket %s", bucket)
	var marker *string
	var listedCount int64 = 0
	for {
		select {
		case <-c.ctx.Done():
			log.Infof("Object scan cancelled for bucket %s.", bucket)
			return
		default:
			result, err := c.sourceS3.ListObjects(bucket, marker)
			if err != nil {
				log.Errorf("Failed to list objects in bucket %s: %v", bucket, err)
				common.ErrorsTotal.WithLabelValues("controller", "list_objects").Inc()
				return
			}

			for _, obj := range result.Contents {
				listedCount++
				// 创建对象迁移任务
				task := workflow.NewMigrateObjectTask(bucket, *obj.Key)
				if err := c.queue.PushTask(c.config.Queue.ObjectMigrationQueue, task); err != nil {
					log.Errorf("Failed to push object migration task for %s/%s: %v", bucket, *obj.Key, err)
					common.ErrorsTotal.WithLabelValues("controller", "push_task").Inc()
					continue
				}
				common.TasksPushed.WithLabelValues(c.config.Queue.ObjectMigrationQueue).Inc()
				log.Debugf("Pushed object migration task for %s/%s", bucket, *obj.Key) // Use Debugf for potentially high volume logs
			}
			common.ObjectsListed.Add(float64(len(result.Contents)))

			if result.NextMarker == nil {
				log.Infof("Finished scanning %d objects in bucket %s", listedCount, bucket)
				return
			}
			marker = result.NextMarker
		}
	}
}

func main() {
	// 1. 加载配置
	config, err := common.LoadConfig("configs/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	// 2. 初始化日志
	if err := common.InitLogger(&config.System); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err)
	}

	// 3. 创建Controller
	controller, err := NewController(config)
	if err != nil {
		log.Fatalf("Failed to create controller: %v", err)
	}

	// 4. 启动Controller
	log.Info("Starting Controller...")
	if err := controller.Start(); err != nil {
		log.Fatalf("Controller failed: %v", err)
	}
	log.Info("Controller stopped.")
}
