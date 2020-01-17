# S3Sooner

Improve performance of s3mover

关于`S3Sooner`的设计文档，请见[S3备份工具](http://wiki.hengchang6.com/pages/viewpage.action?pageId=20514338)

## Build

### Debian

以`debian(buster)`为例说明`S3Sooner`编译过程

#### Pull docker image

```shell
$ docker pull golang:1.13.6-buster
```

#### 获取AWS SDK

获取`AWS SDK`有两种方法，一种通过`go get`命令获取，详细请见["Getting Started with the AWS SDK for Go"](https://docs.aws.amazon.com/zh_cn/sdk-for-go/v1/developer-guide/setting-up.html)。另一种方式是通过`git clone`将aws工程clone到本地，然后再使用docker的`-v`参数将aws工程映射到容器内。个人推荐第二种方法。

```shell
$ docker run --net host --name s3sooner_builder -v $GOPATH/src:/go/src -it golang:1.13.6-buster /bin/bash
```

#### 安装RocksDB

`debian(buster)`发行版默认支持的RocksDB的版本为`5.17`满足我们的应用需要，所以可直接`apt install`

以下命令容器内执行
```shell
# apt update -y
# apt install -y libgflags-dev
# apt install -y libsnappy-dev
# apt install -y zlib1g-dev
# apt install -y libbz2-dev
# apt install -y liblz4-dev
# apt install -y libzstd-dev
# apt install -y librocksdb-dev
```
其它linux发行版RocksDB的安装方法请见[facebook/rocksdb](https://github.com/facebook/rocksdb/blob/master/INSTALL.md)

#### 安装gorocksdb

由于`Rocksdb`原生只支持`C++`和`JAVA`两种接口。而我们使用的是`Golang`为了提高开发效率我们引入了`gorocksdb`第三方库。

安装`gorocksdb`库
```shell
# go get github.com/tecbot/gorocksdb
```
如果您使用的`rocksdb`是源码编译的，`include`和lib没有copy到`/usr/include`和`/usr/lib`目录下。所以需要指定好库目录进行安装
```shell
# CGO_CFLAGS="-I/path/to/rocksdb/include" CGO_LDFLAGS="-L/path/to/rocksdbLIB -lrocksdb -lstdc++ -lm -lz -lbz2 -llz4 -lzstd" go get github.com/tecbot/gorocksdb
```
更详细内容，请见[tecbot/gorocksdb](https://github.com/tecbot/gorocksdb)

#### 编译S3Sooner

进入`S3Sooner`所在目录
如果您使用的是发行版自带的`RocksDB`，可以直接使用`go build`进行编译

```shell
# go build -o s3sooner *.go
```

如果您使用的是自己编译的`RocksDB`，需要指定`rocksdb`lib的连接路径

```shell
# CGO_LDFLAGS="-L/path/to/rocksdbLIB -lrocksdb" go build -o s3sooner *.go
```

## RUN

以`Debian(buster)`为例，展示`S3Sooner`所依赖的库

```shell
# ldd ./s3sooner
linux-vdso.so.1 (0x00007ffd7959d000)
librocksdb.so.5.17 => /usr/lib/librocksdb.so.5.17 (0x00007f01746f8000)
libstdc++.so.6 => /usr/lib/x86_64-linux-gnu/libstdc++.so.6 (0x00007f0174574000)
libm.so.6 => /lib/x86_64-linux-gnu/libm.so.6 (0x00007f01743f1000)
libdl.so.2 => /lib/x86_64-linux-gnu/libdl.so.2 (0x00007f01743ec000)
libpthread.so.0 => /lib/x86_64-linux-gnu/libpthread.so.0 (0x00007f01743cb000)
libc.so.6 => /lib/x86_64-linux-gnu/libc.so.6 (0x00007f017420a000)
librt.so.1 => /lib/x86_64-linux-gnu/librt.so.1 (0x00007f01741fe000)
libsnappy.so.1 => /usr/lib/x86_64-linux-gnu/libsnappy.so.1 (0x00007f0173ff6000)
libgflags.so.2.2 => /usr/lib/x86_64-linux-gnu/libgflags.so.2.2 (0x00007f0173fd0000)
libz.so.1 => /lib/x86_64-linux-gnu/libz.so.1 (0x00007f0173db2000)
libbz2.so.1.0 => /lib/x86_64-linux-gnu/libbz2.so.1.0 (0x00007f0173d9f000)
liblz4.so.1 => /usr/lib/x86_64-linux-gnu/liblz4.so.1 (0x00007f0173d80000)
libzstd.so.1 => /usr/lib/x86_64-linux-gnu/libzstd.so.1 (0x00007f0173cde000)
libgcc_s.so.1 => /lib/x86_64-linux-gnu/libgcc_s.so.1 (0x00007f0173cc4000)
/lib64/ld-linux-x86-64.so.2 (0x00007f0174d8e000)
```
不难看出`S3Sooner`运行所依赖的库都是与`RocksDB`相关的库，所以运行前保证`rocksdb`相关库成功安装。

`S3Sooner`是用来完成S3数据迁移工作的，所以运行前需要配置好`config.json`，`config.json`需要与`S3Sooner`放置在相同目录下。

`config.json` example
```json
{
    "CPUs": 2,
    "WorkerPoolSize": 3,
    "Source": {
        "Endpoint": "http://11.11.11.111:7480",
        "AccessKey": "8IXXXXXXXXXXXXXXXXB0",
        "SecretKey": "AXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXALc"
    },
    "Destination": {
        "Endpoint": "http://12.12.12.112:7480",
        "AccessKey": "XXXXXXXXXXXXXXXXX3M",
        "SecretKey": "PXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXxxJoaOF"
    }
}
```
*** `S3Sooner`运行过程中会在`S3Sooner`所在目录生成`RocksDB`数据和迁移数据的临时数据，使用容器运行的环境建议将此目录映射到宿主机上。 ***

# 参考&鸣谢

* [tecbot/gorocksdb](https://github.com/tecbot/gorocksdb)
* [facebook/rocksdb](https://github.com/facebook/rocksdb)
* [gorocksdb 的安装与使用](https://winjeg.github.io/2018/08/13/storage/rocksdb/)
* [package gorocksdb](https://godoc.org/github.com/tecbot/gorocksdb#Slice)
* [AWS SDK for Go API Reference](https://docs.aws.amazon.com/sdk-for-go/api/)

