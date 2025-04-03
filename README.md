# S3RunFast: 分布式 S3 数据迁移工具

S3RunFast 是一个基于 Go 语言开发的分布式数据迁移工具，旨在高效、可靠地将数据从一个 S3 兼容的对象存储迁移到另一个。它特别设计用于处理大量 Bucket 和 Object 的场景，并保证了 Bucket 创建和 Object 迁移之间的顺序依赖关系。

## 工作原理

S3RunFast 采用两阶段工作流（Two-Phase Workflow）和 Controller-Worker 架构来实现分布式数据迁移：

1.  **架构**:
    *   **Controller**: 负责协调整个迁移过程。它扫描源 S3 的 Bucket 列表，生成 "创建 Bucket" 任务，并监听 Bucket 创建完成事件，然后扫描该 Bucket 下的对象列表，生成 "迁移对象" 任务。
    *   **Worker**: 实际执行任务的工作节点。Worker 可以水平扩展部署多个实例。有两种类型的任务处理逻辑：
        *   **Bucket 创建**: 从队列获取 `CreateBucket` 任务，在目标 S3 上创建对应的 Bucket，并发布 "Bucket 创建完成" 事件。
        *   **对象迁移**: 确认目标 Bucket 已创建后，从队列获取 `MigrateObject` 任务，从源 S3 下载对象，然后上传到目标 S3。
    *   **Redis**: 作为消息队列和状态存储。
        *   **任务队列**: 存储 Controller 生成的 `CreateBucket` 和 `MigrateObject` 任务。
        *   **事件发布/订阅**: 用于 Bucket 创建完成后通知 Controller 开始对象扫描。
        *   **状态存储**: 存储 Bucket 和 Object 的迁移状态（例如，是否完成，最后修改时间等），用于支持幂等性和增量迁移。

2.  **两阶段工作流**:
    *   **阶段一：Bucket 创建**: Controller 生成所有源 Bucket 的创建任务，由 Worker 执行。确保所有目标 Bucket 在对象迁移开始之前都已存在。
    *   **阶段二：对象迁移**: 当一个 Bucket 在目标端创建成功后，Controller 会收到事件通知，然后开始扫描该 Bucket 在源端的对象，并生成对象迁移任务。Worker 获取这些任务并执行实际的对象数据传输。

3.  **顺序保证**: 通过将 Bucket 创建和对象迁移分离到两个阶段，并利用消息队列和事件通知，严格保证了只有在目标 Bucket 成功创建之后，才会开始向该 Bucket 迁移对象，避免了因依赖关系导致的错误。

4.  **可靠性与扩展性**:
    *   **任务队列**: Redis 提供了任务的持久化和缓冲，即使 Controller 或 Worker 暂时失败，任务也不会丢失。
    *   **幂等性**: Bucket 创建和对象迁移操作设计为幂等，重复执行任务不会产生副作用。
    *   **水平扩展**: Worker 节点可以根据负载需要部署多个实例，并行处理任务，提高迁移效率。
    *   **监控**: 通过结构化日志 (logrus) 和 Prometheus 指标提供可观察性。

## 编译方法

确保您的环境中已安装 Go (建议版本 1.18 或更高)。

1.  **克隆仓库**:
    ```bash
    git clone <your-repository-url>
    cd S3RunFast
    ```

2.  **下载依赖**:
    ```bash
    go mod tidy
    ```

3.  **编译 Controller**:
    ```bash
    go build -o bin/controller cmd/controller/main.go
    ```
    编译后的二进制文件将位于 `bin/controller`。

4.  **编译 Worker**:
    ```bash
    go build -o bin/worker cmd/worker/main.go
    ```
    编译后的二进制文件将位于 `bin/worker`。

## 使用方法

1.  **配置 (`configs/config.yaml`)**:
    *   在运行之前，您需要编辑 `configs/config.yaml` 文件，根据您的实际环境配置参数。
    *   **关键配置项**:
        *   `system`: 日志级别 (`log_level`)、日志文件路径 (`log_file`)、Prometheus 指标服务地址 (`metrics_addr`)。
        *   `queue.redis`: Redis 服务器地址、密码、数据库编号，用于任务队列和事件。
        *   `storage.redis`: Redis 服务器地址、密码、数据库编号，用于状态存储。
        *   `source`: 源 S3 兼容存储的 Endpoint、Access Key、Secret Key、Region 等。
        *   `destination`: 目标 S3 兼容存储的 Endpoint、Access Key、Secret Key、Region 等。
        *   `worker.object_migration`: 对象迁移时的分片大小 (`chunk_size`)、临时文件目录 (`temp_dir`)。
    *   **注意**: 确保 Controller 和所有 Worker 实例使用相同的配置文件，或者保证配置文件中的 Redis、S3 设置一致。

2.  **创建日志目录**:
    根据 `config.yaml` 中 `system.log_file` 的配置创建必要的目录，例如：
    ```bash
    mkdir -p logs
    ```

3.  **运行 Controller**:
    启动一个 Controller 实例。
    ```bash
    ./bin/controller
    ```
    Controller 会开始扫描源 S3，并将任务推送到 Redis 队列。

4.  **运行 Worker**:
    您可以根据需要启动一个或多个 Worker 实例来处理任务。
    ```bash
    ./bin/worker
    ```
    或者在不同的机器上启动多个实例：
    ```bash
    # 在机器 A 上
    ./bin/worker

    # 在机器 B 上
    ./bin/worker
    ```
    Worker 会从 Redis 队列中拉取任务并执行。

5.  **监控**:
    *   **日志**: 查看配置文件中指定的日志文件（默认为 `logs/s3runfast.log`）或标准输出，了解 Controller 和 Worker 的运行状态和错误信息。
    *   **Prometheus 指标**: 如果配置了 `metrics_addr` (默认为 `:9091`)，可以通过访问 `http://<controller-or-worker-ip>:<port>/metrics` 来获取 Prometheus 指标，用于监控任务处理速率、错误数量、队列状态等。您可以使用 Prometheus + Grafana 来收集和可视化这些指标。

6.  **停止**:
    可以通过向 Controller 和 Worker 进程发送 `SIGINT` 或 `SIGTERM` 信号（例如，在前台运行时按 `Ctrl+C`）来优雅地停止它们。

## 注意事项

*   **幂等性**: 请确保您的 S3 存储支持幂等操作，特别是 Bucket 创建。对象迁移通过比较最后修改时间来尝试实现幂等。
*   **资源**: 运行 Worker（特别是对象迁移）会消耗网络带宽和磁盘 I/O（用于临时存储下载的对象）。请确保 Worker 运行环境有足够的资源。
*   **错误处理**: 当前的错误处理和重试逻辑比较基础。对于生产环境，可能需要根据具体需求进行增强。
*   **安全性**: 配置文件中包含敏感的 S3 Access Key 和 Secret Key，请妥善保管。建议使用环境变量或专门的密钥管理服务来管理凭证。

