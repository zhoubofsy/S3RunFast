package workflow

import (
	"time"
)

// TaskType 任务类型
type TaskType string

const (
	TaskTypeCreateBucket  TaskType = "CREATE_BUCKET"
	TaskTypeMigrateObject TaskType = "MIGRATE_OBJECT"
)

// TaskStatus 任务状态
type TaskStatus string

const (
	TaskStatusPending  TaskStatus = "PENDING"
	TaskStatusRunning  TaskStatus = "RUNNING"
	TaskStatusComplete TaskStatus = "COMPLETE"
	TaskStatusFailed   TaskStatus = "FAILED"
)

// Task 基础任务结构
type Task struct {
	ID        string     `json:"id"`
	Type      TaskType   `json:"type"`
	Status    TaskStatus `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	Retries   int        `json:"retries"`
	Error     string     `json:"error,omitempty"`
}

// CreateBucketTask 创建桶任务
type CreateBucketTask struct {
	Task
	Bucket string `json:"bucket"`
}

// MigrateObjectTask 迁移对象任务
type MigrateObjectTask struct {
	Task
	Bucket string `json:"bucket"`
	Key    string `json:"key"`
}

// BucketStatus 桶状态
type BucketStatus struct {
	Bucket    string     `json:"bucket"`
	Status    TaskStatus `json:"status"`
	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	Error     string     `json:"error,omitempty"`
}

// ObjectStatus 对象状态
type ObjectStatus struct {
	Bucket       string     `json:"bucket"`
	Key          string     `json:"key"`
	Status       TaskStatus `json:"status"`
	LastModified time.Time  `json:"last_modified"`
	CreatedAt    time.Time  `json:"created_at"`
	UpdatedAt    time.Time  `json:"updated_at"`
	Error        string     `json:"error,omitempty"`
}

// NewCreateBucketTask 创建新的创建桶任务
func NewCreateBucketTask(bucket string) *CreateBucketTask {
	now := time.Now()
	return &CreateBucketTask{
		Task: Task{
			ID:        generateTaskID(TaskTypeCreateBucket, bucket),
			Type:      TaskTypeCreateBucket,
			Status:    TaskStatusPending,
			CreatedAt: now,
			UpdatedAt: now,
		},
		Bucket: bucket,
	}
}

// NewMigrateObjectTask 创建新的迁移对象任务
func NewMigrateObjectTask(bucket, key string) *MigrateObjectTask {
	now := time.Now()
	return &MigrateObjectTask{
		Task: Task{
			ID:        generateTaskID(TaskTypeMigrateObject, bucket+"/"+key),
			Type:      TaskTypeMigrateObject,
			Status:    TaskStatusPending,
			CreatedAt: now,
			UpdatedAt: now,
		},
		Bucket: bucket,
		Key:    key,
	}
}

// generateTaskID 生成任务ID
func generateTaskID(taskType TaskType, identifier string) string {
	return string(taskType) + "_" + identifier
}

// UpdateStatus 更新任务状态
func (t *Task) UpdateStatus(status TaskStatus, err error) {
	t.Status = status
	t.UpdatedAt = time.Now()
	if err != nil {
		t.Error = err.Error()
		t.Retries++
	}
}

// IsComplete 检查任务是否完成
func (t *Task) IsComplete() bool {
	return t.Status == TaskStatusComplete
}

// IsFailed 检查任务是否失败
func (t *Task) IsFailed() bool {
	return t.Status == TaskStatusFailed
}

// CanRetry 检查任务是否可以重试
func (t *Task) CanRetry(maxRetries int) bool {
	return t.Status == TaskStatusFailed && t.Retries < maxRetries
}
