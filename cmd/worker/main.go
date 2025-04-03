package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"S3RunFast/pkg/common"
	"S3RunFast/pkg/queue"
	"S3RunFast/pkg/storage"
	"S3RunFast/pkg/workflow"

	log "github.com/sirupsen/logrus"
)

type Worker struct {
	config   *common.Config
	sourceS3 *storage.S3Client
	destS3   *storage.S3Client
	queue    *queue.RedisQueue
	ctx      context.Context
	cancel   context.CancelFunc
	tempDir  string
}

func NewWorker(config *common.Config) (*Worker, error) {
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

	// 创建临时目录
	tempDir := config.Worker.ObjectMigration.TempDir
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create temp directory: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Worker{
		config:   config,
		sourceS3: sourceS3,
		destS3:   destS3,
		queue:    redisQueue,
		ctx:      ctx,
		cancel:   cancel,
		tempDir:  tempDir,
	}, nil
}

func (w *Worker) Start() error {
	// 1. 启动桶创建任务处理
	go w.processBucketCreationTasks()

	// 2. 启动对象迁移任务处理
	go w.processObjectMigrationTasks()

	// 3. 启动 Metrics Server (if configured)
	if w.config.System.MetricsAddr != "" {
		common.StartMetricsServer(w.config.System.MetricsAddr)
	}

	// 4. 等待中断信号
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	// 5. 优雅关闭
	log.Info("Shutting down worker...")
	w.Shutdown()
	return nil
}

func (w *Worker) Shutdown() {
	w.cancel()
	w.queue.Close()
}

// processBucketCreationTasks 处理桶创建任务
func (w *Worker) processBucketCreationTasks() {
	workerType := "bucket_creator"
	queueName := w.config.Queue.BucketCreationQueue
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			var task workflow.CreateBucketTask
			if err := w.queue.PopTask(queueName, &task); err != nil {
				if err.Error() != "queue is empty" {
					log.Warnf("Failed to pop bucket creation task: %v", err)
					common.ErrorsTotal.WithLabelValues(workerType, "pop_task").Inc()
				}
				time.Sleep(time.Second)
				continue
			}
			common.TasksPopped.WithLabelValues(queueName).Inc()
			log.Infof("Processing bucket creation task for %s", task.Bucket)

			// 更新任务状态为运行中
			task.UpdateStatus(workflow.TaskStatusRunning, nil)
			if err := w.queue.SetStatus(w.config.Storage.BucketStatusPrefix+task.Bucket, task, 24*time.Hour); err != nil {
				log.Errorf("Failed to update task status to running for %s: %v", task.Bucket, err)
				common.ErrorsTotal.WithLabelValues(workerType, "update_status").Inc()
				// Attempt to requeue or handle error appropriately
				continue
			}

			// 执行桶创建
			if err := w.destS3.CreateBucket(task.Bucket); err != nil {
				log.Errorf("Failed to create bucket %s: %v", task.Bucket, err)
				common.ErrorsTotal.WithLabelValues(workerType, "create_bucket").Inc()
				task.UpdateStatus(workflow.TaskStatusFailed, err)
				if err := w.queue.SetStatus(w.config.Storage.BucketStatusPrefix+task.Bucket, task, 24*time.Hour); err != nil {
					log.Errorf("Failed to update task status to failed for %s: %v", task.Bucket, err)
					common.ErrorsTotal.WithLabelValues(workerType, "update_status").Inc()
				}
				common.TasksProcessed.WithLabelValues(workerType, "failed").Inc()
				// Handle retry logic if needed
				continue
			}

			// 更新任务状态为完成
			task.UpdateStatus(workflow.TaskStatusComplete, nil)
			if err := w.queue.SetStatus(w.config.Storage.BucketStatusPrefix+task.Bucket, task, 24*time.Hour); err != nil {
				log.Errorf("Failed to update task status to complete for %s: %v", task.Bucket, err)
				common.ErrorsTotal.WithLabelValues(workerType, "update_status").Inc()
				// Potentially inconsistent state, needs monitoring/alerting
				continue
			}

			// 发布桶创建完成事件
			event := struct {
				Bucket string `json:"bucket"`
				Status string `json:"status"`
			}{
				Bucket: task.Bucket,
				Status: "created",
			}
			if err := w.queue.PublishEvent(w.config.Queue.BucketCreationTopic, event); err != nil {
				log.Errorf("Failed to publish bucket creation event for %s: %v", task.Bucket, err)
				common.ErrorsTotal.WithLabelValues(workerType, "publish_event").Inc()
			}
			common.EventsPublished.WithLabelValues(w.config.Queue.BucketCreationTopic).Inc()
			common.TasksProcessed.WithLabelValues(workerType, "completed").Inc()
			log.Infof("Successfully created bucket %s", task.Bucket)
		}
	}
}

// processObjectMigrationTasks 处理对象迁移任务
func (w *Worker) processObjectMigrationTasks() {
	workerType := "object_migrator"
	queueName := w.config.Queue.ObjectMigrationQueue
	for {
		select {
		case <-w.ctx.Done():
			return
		default:
			var task workflow.MigrateObjectTask
			if err := w.queue.PopTask(queueName, &task); err != nil {
				if err.Error() != "queue is empty" { // Avoid logging empty queue errors excessively
					log.Warnf("Failed to pop object migration task: %v", err)
					common.ErrorsTotal.WithLabelValues(workerType, "pop_task").Inc()
				}
				time.Sleep(time.Second)
				continue
			}
			common.TasksPopped.WithLabelValues(queueName).Inc()
			log.Infof("Processing object migration task for %s/%s", task.Bucket, task.Key)

			// 检查桶状态
			var bucketStatus workflow.BucketStatus
			if err := w.queue.GetStatus(w.config.Storage.BucketStatusPrefix+task.Bucket, &bucketStatus); err != nil {
				log.Warnf("Failed to get bucket status for %s (task %s): %v. Requeuing task.", task.Bucket, task.ID, err)
				common.ErrorsTotal.WithLabelValues(workerType, "get_bucket_status").Inc()
				if err := w.requeueTask(queueName, task); err != nil {
					log.Errorf("Failed to requeue task %s: %v", task.ID, err)
					common.ErrorsTotal.WithLabelValues(workerType, "requeue_task").Inc()
				}
				continue
			}

			if bucketStatus.Status != workflow.TaskStatusComplete {
				log.Infof("Bucket %s is not ready for object migration (task %s). Requeuing task.", task.Bucket, task.ID)
				if err := w.requeueTask(queueName, task); err != nil {
					log.Errorf("Failed to requeue task %s: %v", task.ID, err)
					common.ErrorsTotal.WithLabelValues(workerType, "requeue_task").Inc()
				}
				continue
			}

			objectStatusKey := w.config.Storage.ObjectStatusPrefix + task.Bucket + "/" + task.Key
			// 更新任务状态为运行中
			task.UpdateStatus(workflow.TaskStatusRunning, nil)
			if err := w.queue.SetStatus(objectStatusKey, task, 24*time.Hour); err != nil {
				log.Errorf("Failed to update task status to running for %s/%s: %v", task.Bucket, task.Key, err)
				common.ErrorsTotal.WithLabelValues(workerType, "update_status").Inc()
				// Attempt to requeue or handle error appropriately
				continue
			}

			// 获取源对象信息
			sourceObjInfo, err := w.sourceS3.HeadObject(task.Bucket, task.Key) // Using HeadObject for efficiency
			if err != nil {
				log.Errorf("Failed to get source object info for %s/%s: %v", task.Bucket, task.Key, err)
				common.ErrorsTotal.WithLabelValues(workerType, "head_source_object").Inc()
				task.UpdateStatus(workflow.TaskStatusFailed, err)
				if err := w.queue.SetStatus(objectStatusKey, task, 24*time.Hour); err != nil {
					log.Errorf("Failed to update task status to failed for %s/%s: %v", task.Bucket, task.Key, err)
					common.ErrorsTotal.WithLabelValues(workerType, "update_status").Inc()
				}
				common.TasksProcessed.WithLabelValues(workerType, "failed").Inc()
				continue
			}
			lastModified := *sourceObjInfo.LastModified
			objSize := *sourceObjInfo.ContentLength

			// 检查是否需要迁移 (Compare with existing status if available)
			var existingObjectStatus workflow.ObjectStatus
			if err := w.queue.GetStatus(objectStatusKey, &existingObjectStatus); err == nil {
				// Check if the existing migrated object is the same or newer
				if !existingObjectStatus.LastModified.IsZero() && !existingObjectStatus.LastModified.Before(lastModified) {
					log.Infof("Object %s/%s is already up to date. Skipping.", task.Bucket, task.Key)
					task.UpdateStatus(workflow.TaskStatusComplete, nil)
					if err := w.queue.SetStatus(objectStatusKey, task, 24*time.Hour); err != nil {
						log.Errorf("Failed to update task status to complete (skipped) for %s/%s: %v", task.Bucket, task.Key, err)
						common.ErrorsTotal.WithLabelValues(workerType, "update_status").Inc()
					}
					common.TasksProcessed.WithLabelValues(workerType, "skipped").Inc()
					continue
				}
			} else if err.Error() != "status not found" {
				log.Warnf("Could not get existing object status for %s/%s: %v. Proceeding with migration.", task.Bucket, task.Key, err)
				common.ErrorsTotal.WithLabelValues(workerType, "get_object_status").Inc()
			}

			// 创建临时文件路径
			tempFile := filepath.Join(w.tempDir, task.Bucket, task.Key)
			tempDir := filepath.Dir(tempFile)
			if err := os.MkdirAll(tempDir, 0755); err != nil {
				log.Errorf("Failed to create temporary directory %s for %s/%s: %v", tempDir, task.Bucket, task.Key, err)
				common.ErrorsTotal.WithLabelValues(workerType, "create_temp_dir").Inc()
				task.UpdateStatus(workflow.TaskStatusFailed, err)
				if err := w.queue.SetStatus(objectStatusKey, task, 24*time.Hour); err != nil {
					log.Errorf("Failed to update task status to failed for %s/%s: %v", task.Bucket, task.Key, err)
					common.ErrorsTotal.WithLabelValues(workerType, "update_status").Inc()
				}
				common.TasksProcessed.WithLabelValues(workerType, "failed").Inc()
				continue
			}

			// 下载对象
			if err := w.sourceS3.DownloadLargeObject(task.Bucket, task.Key, tempFile); err != nil {
				log.Errorf("Failed to download object %s/%s to %s: %v", task.Bucket, task.Key, tempFile, err)
				common.ErrorsTotal.WithLabelValues(workerType, "download_object").Inc()
				task.UpdateStatus(workflow.TaskStatusFailed, err)
				if err := w.queue.SetStatus(objectStatusKey, task, 24*time.Hour); err != nil {
					log.Errorf("Failed to update task status to failed for %s/%s: %v", task.Bucket, task.Key, err)
					common.ErrorsTotal.WithLabelValues(workerType, "update_status").Inc()
				}
				os.Remove(tempFile) // Clean up partial download
				common.TasksProcessed.WithLabelValues(workerType, "failed").Inc()
				continue
			}

			// 上传对象
			if err := w.destS3.UploadLargeObject(task.Bucket, task.Key, tempFile, w.config.Worker.ObjectMigration.ChunkSize); err != nil {
				log.Errorf("Failed to upload object %s/%s from %s: %v", task.Bucket, task.Key, tempFile, err)
				common.ErrorsTotal.WithLabelValues(workerType, "upload_object").Inc()
				task.UpdateStatus(workflow.TaskStatusFailed, err)
				if err := w.queue.SetStatus(objectStatusKey, task, 24*time.Hour); err != nil {
					log.Errorf("Failed to update task status to failed for %s/%s: %v", task.Bucket, task.Key, err)
					common.ErrorsTotal.WithLabelValues(workerType, "update_status").Inc()
				}
				os.Remove(tempFile) // Clean up downloaded file even on upload failure
				common.TasksProcessed.WithLabelValues(workerType, "failed").Inc()
				continue
			}

			// 删除临时文件
			if err := os.Remove(tempFile); err != nil {
				log.Warnf("Failed to remove temp file %s: %v", tempFile, err)
				common.ErrorsTotal.WithLabelValues(workerType, "remove_temp_file").Inc()
			}

			// 更新任务状态为完成 (Important: Set LastModified from source object)
			task.UpdateStatus(workflow.TaskStatusComplete, nil)
			// We need to store the ObjectStatus struct now, not just the base task
			completedStatus := workflow.ObjectStatus{
				Bucket:       task.Bucket,
				Key:          task.Key,
				Status:       task.Status,
				LastModified: lastModified,
				CreatedAt:    task.CreatedAt, // Keep original creation time
				UpdatedAt:    task.UpdatedAt,
			}
			if err := w.queue.SetStatus(objectStatusKey, completedStatus, 24*time.Hour); err != nil {
				log.Errorf("Failed to update task status to complete for %s/%s: %v", task.Bucket, task.Key, err)
				common.ErrorsTotal.WithLabelValues(workerType, "update_status").Inc()
				// Potentially inconsistent state, needs monitoring/alerting
				continue
			}
			common.ObjectsMigratedBytes.Add(float64(objSize))
			common.TasksProcessed.WithLabelValues(workerType, "completed").Inc()
			log.Infof("Successfully migrated object %s/%s (%d bytes)", task.Bucket, task.Key, objSize)
		}
	}
}

// requeueTask pushes a task back to the front of the queue (L<y_bin_540>PUSH)
func (w *Worker) requeueTask(queueName string, task interface{}) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task for requeue: %v", err)
	}
	ctx := context.Background()
	// Use LPush to put it back at the front for faster retry
	if err := w.queue.LPush(ctx, queueName, data).Err(); err != nil {
		return fmt.Errorf("failed to lpush task to queue: %v", err)
	}
	return nil
}

func main() {
	// 1. 加载配置
	config, err := common.LoadConfig("configs/config.yaml")
	if err != nil {
		log.Fatalf("Failed to load config: %v", err) // Use standard log
	}

	// 2. 初始化日志
	if err := common.InitLogger(&config.System); err != nil {
		log.Fatalf("Failed to initialize logger: %v", err) // Use standard log
	}

	// 3. 创建Worker
	worker, err := NewWorker(config)
	if err != nil {
		log.Fatalf("Failed to create worker: %v", err)
	}

	// 4. 启动Worker
	log.Info("Starting Worker...")
	if err := worker.Start(); err != nil {
		log.Fatalf("Worker failed: %v", err)
	}
	log.Info("Worker stopped.")
}
