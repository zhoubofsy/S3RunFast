package main

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"
)

type Job struct {
	Src    *S3Executer
	Dest   *S3Executer
	Bucket string
	Object string
	Sts    *Stat
}

func worker(id int, j *Job, d *Dispatcher) {
	defer d.complete(id)
	if j.Src == nil || j.Dest == nil {
		return
	}

	st := j.Sts
	// Compare LastModified
	obj_ts := j.Src.get_obj_last_modify(j.Bucket, j.Object)
	if obj_ts == 0 {
		return
	}
	sts_ts, _ := st.GetLastMT(j.Bucket, j.Object)
	if obj_ts == sts_ts {
		fmt.Printf("worker(%d) NO need to move bucket: %v, object: %v, ts: (%d, %d)\n", id, j.Bucket, j.Object, obj_ts, sts_ts)
		return
	}
	//fmt.Printf("worker(%d) prepare move bucket: %v, object: %v, ts: (%d, %d)\n", id, j.Bucket, j.Object, obj_ts, sts_ts)
	var buffer []byte
	// download object
	file, err := j.Src.GetToBuf(j.Bucket, j.Object, &buffer)
	if err != nil {
		fmt.Printf("Download object[%v/%v] error : %v\n", j.Bucket, j.Object, err)
		return
	}
	// upload object
	if file == "" {
		// upload buffer
		err = j.Dest.PutFromBuf(j.Bucket, j.Object, &buffer)
		if err != nil {
			fmt.Printf("Upload object[%v/%v] error : %v\n", j.Bucket, j.Object, err)
		}
	} else {
		// upload file via multipart
		err = j.Dest.PutFromFile(j.Bucket, j.Object, file)
		if err != nil {
			fmt.Printf("Upload object[%v/%v] from %v , error : %v \n", j.Bucket, j.Object, file, err)
		}
		err = os.Remove(file)
		if err != nil {
			fmt.Printf("Remove file: %v , error ! %v \n", file, err)
		}
	}
	// update stat
	err = st.UpdateLastMT(j.Bucket, j.Object, obj_ts)
	fmt.Printf("worker(%d) move bucket: %v, object: %v \n", id, j.Bucket, j.Object)
	//time.Sleep(3 * time.Second)
}

func waiter(d *Dispatcher) {
	for {
		select {
		case workermsg := <-d.WorkerChannel:
			{
				d.release(workermsg)
			}
		case waitermsg := <-d.WaiterChannel:
			{
				fmt.Println("waiter msg : ", waitermsg)
			}
		}
	}
}

type WorkerStatus int

const (
	WS_UNUSE WorkerStatus = 0
	WS_USED  WorkerStatus = 1
)

type Dispatcher struct {
	WorkPoolSz    int
	count         int
	WorkerChannel chan int
	WaiterChannel chan int
	Workers       []WorkerStatus
	l             sync.Mutex
}

func (this *Dispatcher) LockWorkers() {
	this.l.Lock()
}
func (this *Dispatcher) UnlockWorkers() {
	this.l.Unlock()
}
func (this *Dispatcher) init(size int) {
	this.WorkPoolSz = size
	this.count = 0
	this.WorkerChannel = make(chan int, this.WorkPoolSz)
	this.WaiterChannel = make(chan int)
	this.Workers = make([]WorkerStatus, this.WorkPoolSz, this.WorkPoolSz)
	go waiter(this)
}

func (this *Dispatcher) release(id int) {
	this.LockWorkers()
	if this.Workers[id] == WS_UNUSE {
		this.UnlockWorkers()
		return
	}
	this.Workers[id] = WS_UNUSE
	if this.count > 0 {
		this.count -= 1
	}
	//fmt.Printf("release id : %d, count: %d, PoolSize: %d\n", id, this.count, this.WorkPoolSz)
	this.UnlockWorkers()
}

func (this *Dispatcher) allocate() (int, error) {
	id := -1
	err := errors.New("NOT ENOUGH")
	this.LockWorkers()
	if this.count < this.WorkPoolSz {
		for i := 0; i < this.WorkPoolSz; i++ {
			if this.Workers[i] == WS_UNUSE {
				this.Workers[i] = WS_USED
				this.count += 1
				id = i
				err = nil
				//fmt.Printf("allocate id : %d, count: %d, PoolSize: %d\n", id, this.count, this.WorkPoolSz)
				break
			}
		}
	}
	this.UnlockWorkers()
	return id, err
}
func (this *Dispatcher) is_finish() bool {
	return this.count <= 0
}
func (this *Dispatcher) complete(id int) {
	this.WorkerChannel <- id
}
func (this *Dispatcher) sync_dispatch(s *S3Executer, d *S3Executer, bkt string, obj string, sts *Stat) error {
	// create job
	j := new(Job)
	j.Src = s
	j.Dest = d
	j.Bucket = bkt
	j.Object = obj
	j.Sts = sts

	// dispatch the job
	for {
		id, err := this.allocate()
		if err == nil {
			go worker(id, j, this)
			break
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	return nil
}
