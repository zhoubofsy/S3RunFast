package main

import (
	"errors"
	"fmt"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

type SyncBucket struct {
	Src  *S3Executer
	Dest *S3Executer
}

func (this *SyncBucket) init_sync(s *S3Executer, d *S3Executer) error {
	this.Src = s
	this.Dest = d
	return nil
}

func (this *SyncBucket) do_sync(s string, d string) error {
	err := this.sync_bucket(s, d)
	if err == nil {
		this.sync_policy(s, d)
	}
	return err
}

func (this *SyncBucket) sync_bucket(s string, d string) error {
	if this.Dest.is_existed(d) {
		return nil
	}
	if this.Src.is_existed(s) {
		err := this.Dest.create_bucket(d)
		return err
	}
	return errors.New("SourceBucketNotExisted")
}

func (this *SyncBucket) sync_policy(s string, d string) error {
	policy, err := this.Src.get_bucket_policy(s)
	if err != nil {
		if reqerr, ok := err.(awserr.RequestFailure); ok {
			switch reqerr.StatusCode() {
			case 404:
				{
					// try delete dest bucket policy
					_, err = this.Dest.get_bucket_policy(d)
					if err == nil {
						this.Dest.del_bucket_policy(d)
					}
				}
			}
			return nil
		}
		fmt.Printf("sync_policy from %v to %v error : %v\n", s, d, err)
	}
	// 200,OK Set dest bucket policy
	err = this.Dest.put_bucket_policy(d, policy)
	return err
}

func (this *SyncBucket) finish_sync() error {
	return nil
}

func main() {
	dbpath := "dbs"
	if _, err := os.Stat(dbpath); os.IsNotExist(err) {
		os.Mkdir(dbpath, os.ModePerm)
	}
	// Parse Config
	conf := new(Config)
	conf.load("./config.json")

	wkpoolsz := conf.WorkerPoolSz

	// init dispatcher
	dp := new(Dispatcher)
	dp.init(wkpoolsz)

	src := &S3Executer{
		Endpoint: conf.Src.Endpoint,
		ASKeys: AccessSecretKeys{
			AccessKey: conf.Src.Access,
			SecretKey: conf.Src.Secret,
		},
	}

	dest := &S3Executer{
		Endpoint: conf.Dest.Endpoint,
		ASKeys: AccessSecretKeys{
			AccessKey: conf.Dest.Access,
			SecretKey: conf.Dest.Secret,
		},
	}

	bkt_sync := new(SyncBucket)
	bkt_sync.init_sync(src, dest)

	svc, err := src.Init()
	if err != nil {
		fmt.Printf("src Init Error : %v \n", err)
		return
	}

	input := &s3.ListBucketsInput{}
	buckets, err := svc.ListBuckets(input)
	if err != nil {
		fmt.Printf("ListBucket Error : %v \n", err)
	}
	for _, bkt := range buckets.Buckets {
		current_bkt := *(bkt.Name)
		bkt_sync.do_sync(current_bkt, current_bkt)
		sts := new(Stat)
		sts.Init(current_bkt)
		var marker *string
		for true {
			obj_input := &s3.ListObjectsInput{
				Bucket:  aws.String(current_bkt),
				MaxKeys: aws.Int64((int64)(wkpoolsz)),
				Marker:  marker,
			}
			objs, err := svc.ListObjects(obj_input)
			if err != nil {
				break
			}
			marker = objs.NextMarker
			for _, obj := range objs.Contents {
				//fmt.Printf("Bucket: %v, Key: %v, LastModify: %v\n", current_bkt, *(obj.Key), obj.LastModified)
				dp.sync_dispatch(src, dest, current_bkt, *(obj.Key), sts)
			}
			if marker == nil {
				break
			}
		}
	}
	// wait for dispatcher
	for !dp.is_finish() {
		time.Sleep(1 * time.Second)
	}
}
