package common

import (
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	System      SystemConfig      `yaml:"system"`
	Coordinator CoordinatorConfig `yaml:"coordinator"`
	Queue       QueueConfig       `yaml:"queue"`
	Storage     StorageConfig     `yaml:"storage"`
	Source      S3Config          `yaml:"source"`
	Destination S3Config          `yaml:"destination"`
	Controller  ControllerConfig  `yaml:"controller"`
	Worker      WorkerConfig      `yaml:"worker"`
}

type SystemConfig struct {
	LogLevel    string `yaml:"log_level"`
	LogFile     string `yaml:"log_file"`
	MetricsAddr string `yaml:"metrics_addr"`
}

type CoordinatorConfig struct {
	Endpoints   []string      `yaml:"endpoints"`
	DialTimeout time.Duration `yaml:"dial_timeout"`
	Prefix      string        `yaml:"prefix"`
}

type QueueConfig struct {
	Redis                RedisConfig `yaml:"redis"`
	BucketCreationQueue  string      `yaml:"bucket_creation_queue"`
	ObjectMigrationQueue string      `yaml:"object_migration_queue"`
	BucketCreationTopic  string      `yaml:"bucket_creation_topic"`
}

type StorageConfig struct {
	Redis              RedisConfig `yaml:"redis"`
	ObjectStatusPrefix string      `yaml:"object_status_prefix"`
	BucketStatusPrefix string      `yaml:"bucket_status_prefix"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Password string `yaml:"password"`
	DB       int    `yaml:"db"`
}

type S3Config struct {
	Endpoint       string `yaml:"endpoint"`
	AccessKey      string `yaml:"access_key"`
	SecretKey      string `yaml:"secret_key"`
	Region         string `yaml:"region"`
	ForcePathStyle bool   `yaml:"force_path_style"`
}

type ControllerConfig struct {
	WorkerPoolSize int           `yaml:"worker_pool_size"`
	BatchSize      int           `yaml:"batch_size"`
	MaxRetries     int           `yaml:"max_retries"`
	RetryDelay     time.Duration `yaml:"retry_delay"`
}

type WorkerConfig struct {
	BucketCreation  BucketCreationConfig  `yaml:"bucket_creation"`
	ObjectMigration ObjectMigrationConfig `yaml:"object_migration"`
}

type BucketCreationConfig struct {
	PoolSize   int           `yaml:"pool_size"`
	MaxRetries int           `yaml:"max_retries"`
	RetryDelay time.Duration `yaml:"retry_delay"`
}

type ObjectMigrationConfig struct {
	PoolSize   int           `yaml:"pool_size"`
	MaxRetries int           `yaml:"max_retries"`
	RetryDelay time.Duration `yaml:"retry_delay"`
	ChunkSize  int64         `yaml:"chunk_size"`
	TempDir    string        `yaml:"temp_dir"`
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var config Config
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}
