package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"S3RunFast/pkg/common"

	"github.com/go-redis/redis/v8"
)

type RedisQueue struct {
	client *redis.Client
	config *common.QueueConfig
}

func NewRedisQueue(config *common.QueueConfig) (*RedisQueue, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     config.Redis.Addr,
		Password: config.Redis.Password,
		DB:       config.Redis.DB,
	})

	// 测试连接
	ctx := context.Background()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("failed to connect to Redis: %v", err)
	}

	return &RedisQueue{
		client: client,
		config: config,
	}, nil
}

// PushTask 推送任务到队列
func (q *RedisQueue) PushTask(queueName string, task interface{}) error {
	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("failed to marshal task: %v", err)
	}

	ctx := context.Background()
	if err := q.client.RPush(ctx, queueName, data).Err(); err != nil {
		return fmt.Errorf("failed to push task to queue: %v", err)
	}

	return nil
}

// LPush pushes a task to the left (front) of the queue.
func (q *RedisQueue) LPush(ctx context.Context, queueName string, data []byte) *redis.IntCmd {
	return q.client.LPush(ctx, queueName, data)
}

// PopTask 从队列中获取任务
func (q *RedisQueue) PopTask(queueName string, task interface{}) error {
	ctx := context.Background()
	data, err := q.client.LPop(ctx, queueName).Bytes()
	if err == redis.Nil {
		return fmt.Errorf("queue is empty")
	}
	if err != nil {
		return fmt.Errorf("failed to pop task from queue: %v", err)
	}

	if err := json.Unmarshal(data, task); err != nil {
		return fmt.Errorf("failed to unmarshal task: %v", err)
	}

	return nil
}

// PublishEvent 发布事件到主题
func (q *RedisQueue) PublishEvent(topic string, event interface{}) error {
	data, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal event: %v", err)
	}

	ctx := context.Background()
	if err := q.client.Publish(ctx, topic, data).Err(); err != nil {
		return fmt.Errorf("failed to publish event: %v", err)
	}

	return nil
}

// SubscribeEvent 订阅主题事件
func (q *RedisQueue) SubscribeEvent(topic string, handler func([]byte) error) error {
	ctx := context.Background()
	pubsub := q.client.Subscribe(ctx, topic)
	defer pubsub.Close()

	ch := pubsub.Channel()

	for {
		select {
		case msg := <-ch:
			if err := handler([]byte(msg.Payload)); err != nil {
				fmt.Printf("Error handling event: %v\n", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// SetStatus 设置状态
func (q *RedisQueue) SetStatus(key string, value interface{}, expiration time.Duration) error {
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal status: %v", err)
	}

	ctx := context.Background()
	if err := q.client.Set(ctx, key, data, expiration).Err(); err != nil {
		return fmt.Errorf("failed to set status: %v", err)
	}

	return nil
}

// GetStatus 获取状态
func (q *RedisQueue) GetStatus(key string, value interface{}) error {
	ctx := context.Background()
	data, err := q.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return fmt.Errorf("status not found")
	}
	if err != nil {
		return fmt.Errorf("failed to get status: %v", err)
	}

	if err := json.Unmarshal(data, value); err != nil {
		return fmt.Errorf("failed to unmarshal status: %v", err)
	}

	return nil
}

// DeleteStatus 删除状态
func (q *RedisQueue) DeleteStatus(key string) error {
	ctx := context.Background()
	if err := q.client.Del(ctx, key).Err(); err != nil {
		return fmt.Errorf("failed to delete status: %v", err)
	}

	return nil
}

// Close 关闭连接
func (q *RedisQueue) Close() error {
	return q.client.Close()
}
