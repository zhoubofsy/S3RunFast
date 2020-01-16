package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type AccessSecretKeys struct {
	AccessKey string
	SecretKey string
}

func (this *AccessSecretKeys) Retrieve() (credentials.Value, error) {
	return credentials.Value{
		AccessKeyID:     this.AccessKey,
		SecretAccessKey: this.SecretKey,
	}, nil
}

func (this *AccessSecretKeys) IsExpired() bool {
	return false
}

type S3Executer struct {
	Endpoint  string
	ASKeys    AccessSecretKeys
	pathStyle bool
}

func (this *S3Executer) Init() (*s3.S3, error) {
	this.pathStyle = true
	sess := session.Must(session.NewSessionWithOptions(session.Options{
		Config: aws.Config{
			Region:           aws.String("default"),
			Endpoint:         &this.Endpoint,
			S3ForcePathStyle: &this.pathStyle,
			Credentials:      credentials.NewCredentials(&this.ASKeys),
		},
	}))
	return s3.New(sess), nil
}

/*
* Save object to file, when more than 10MB
* Save object to buffer, when less than 10MB
**/
func (this *S3Executer) GetToBuf(bucket string, obj string, buf *[]byte) (string, error) {
	svc, err := this.Init()
	if err != nil {
		fmt.Println(err.Error())
		return "", err
	}
	resp, err := svc.GetObject(&s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &obj,
	})
	if err != nil {
		fmt.Println(err.Error())
		return "", err
	}
	//fmt.Println("ContentLength:", *resp.ContentLength)
	if *(resp.ContentLength) <= 10*1024*1024 {
		*buf, err = ioutil.ReadAll(resp.Body)
		return "", err
	}
	filepath := "./" + bucket
	if _, err = os.Stat(filepath); os.IsNotExist(err) {
		os.Mkdir(filepath, os.ModePerm)
	}
	filepath += ("/" + strings.Replace(obj, "/", "_", -1))
	f_handle, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR, 0666)
	if f_handle != nil {
		_, err := io.Copy(f_handle, resp.Body)
		if err != nil {
			fmt.Printf("write error %v \n", err)
			return "", err
		}
		return filepath, err
	}
	return "", err
}

func (this *S3Executer) PutFromBuf(bucket string, obj string, buf *[]byte) error {
	svc, err := this.Init()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}

	input := &s3.PutObjectInput{
		Body:   aws.ReadSeekCloser(bytes.NewReader(*buf)),
		Bucket: aws.String(bucket),
		Key:    aws.String(obj),
	}
	_, err = svc.PutObject(input)
	if err != nil {
		fmt.Printf("PutObject Error : %v \n", err.Error())
	}
	return err
}

func (this *S3Executer) PutFromFile(bucket string, obj string, file string) error {
	svc, err := this.Init()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	// create multipart
	param_init := &s3.CreateMultipartUploadInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(obj),
	}
	resp_init, err := svc.CreateMultipartUpload(param_init)
	if err != nil {
		fmt.Printf("CreateMultipartUpload Error : %v \n", err.Error())
		return err
	}

	// upload multipart
	f, err := os.Open(file)
	if err != nil {
		fmt.Printf("Open %v , error : %v\n", file, err)
		return err
	}
	defer f.Close()
	bfRd := bufio.NewReader(f)
	buf := make([]byte, 10*1024*1024)
	var part_num int64 = 0
	var completes []*s3.CompletedPart
	for {
		n, err := bfRd.Read(buf)
		if err == io.EOF {
			fmt.Println("read data finished")
			break
		}
		if int64(n) != (10 * 1024 * 1024) {
			data := make([]byte, n)
			data = buf[0:n]
			buf = data
		}
		part_num++
		param := &s3.UploadPartInput{
			Bucket:        aws.String(bucket),
			Key:           aws.String(obj),
			PartNumber:    aws.Int64(part_num),
			UploadId:      resp_init.UploadId,
			Body:          aws.ReadSeekCloser(bytes.NewReader(buf)),
			ContentLength: aws.Int64(int64(n)),
		}
		resp_up, err := svc.UploadPart(param)
		if err != nil {
			fmt.Printf("UploadPart Error : %v \n", err.Error())
			return err
		}
		var c s3.CompletedPart
		c.PartNumber = aws.Int64(part_num)
		c.ETag = resp_up.ETag
		completes = append(completes, &c)
	}

	// complete multipart
	params := &s3.CompleteMultipartUploadInput{
		Bucket:   aws.String(bucket),
		Key:      aws.String(obj),
		UploadId: resp_init.UploadId,
		MultipartUpload: &s3.CompletedMultipartUpload{
			Parts: completes,
		},
	}
	_, err = svc.CompleteMultipartUpload(params)
	if err != nil {
		fmt.Printf("CompleteMultipartUpload Error : %v \n", err.Error())
		return err
	}
	//fmt.Println(resp_comp)
	return nil
}

func (this *S3Executer) is_existed(bkt string) bool {
	svc, err := this.Init()
	if err != nil {
		fmt.Println(err.Error())
		return false
	}
	input := &s3.HeadBucketInput{
		Bucket: aws.String(bkt),
	}
	_, err = svc.HeadBucket(input)
	if err != nil {
		fmt.Println("HeadBucket Error : ", err.Error())
		if reqerr, ok := err.(awserr.RequestFailure); ok {
			switch reqerr.StatusCode() {
			case 404:
				return false
			case 403:
				return true
			}
		}
		return false
	}
	return true
}

func (this *S3Executer) create_bucket(bkt string) error {
	svc, err := this.Init()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	input := &s3.CreateBucketInput{
		Bucket: aws.String(bkt),
		CreateBucketConfiguration: &s3.CreateBucketConfiguration{
			LocationConstraint: aws.String(""),
		},
	}
	_, err = svc.CreateBucket(input)
	if err != nil {
		fmt.Println("CreateBucket Error : ", err.Error())
	}
	return err
}

func (this *S3Executer) get_bucket_policy(bkt string) (string, error) {
	svc, err := this.Init()
	if err != nil {
		fmt.Println(err.Error())
		return "", err
	}
	input := &s3.GetBucketPolicyInput{
		Bucket: aws.String(bkt),
	}
	result, err := svc.GetBucketPolicy(input)
	if err != nil {
		//fmt.Printf("GetBucketPolicy Error %v : %v \n", bkt, err.Error())
		return "", err
	}
	return *(result.Policy), err
}

func (this *S3Executer) put_bucket_policy(bkt string, p string) error {
	svc, err := this.Init()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	input := &s3.PutBucketPolicyInput{
		Bucket: aws.String(bkt),
		Policy: aws.String(p),
	}
	_, err = svc.PutBucketPolicy(input)
	if err != nil {
		fmt.Println(err.Error())
	}
	//fmt.Println(result)
	return err
}
func (this *S3Executer) del_bucket_policy(bkt string) error {
	svc, err := this.Init()
	if err != nil {
		fmt.Println(err.Error())
		return err
	}
	input := &s3.DeleteBucketPolicyInput{
		Bucket: aws.String(bkt),
	}
	_, err = svc.DeleteBucketPolicy(input)
	if err != nil {
		fmt.Println(err.Error())
	}
	return err
}

func (this *S3Executer) get_obj_last_modify(bkt string, obj string) int64 {
	svc, err := this.Init()
	if err != nil {
		fmt.Println(err.Error())
		return 0
	}
	input := &s3.HeadObjectInput{
		Bucket: aws.String(bkt),
		Key:    aws.String(obj),
	}
	result, err := svc.HeadObject(input)
	if err != nil {
		fmt.Println(err.Error())
		return 0
	}
	return result.LastModified.Unix()
}
