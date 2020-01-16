package main

import (
	"encoding/binary"
	"fmt"
	"sync"

	"github.com/tecbot/gorocksdb"
)

func Int64ToBytes(i int64) []byte {
	var buf = make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

func BytesToInt64(buf []byte) int64 {
	if buf == nil {
		return 0
	}
	return int64(binary.BigEndian.Uint64(buf))
}

type Stat struct {
	n string
	s StatDB
}

func (this *Stat) Init(bkt string) {
	this.s = new(StatRocksDB)
	this.n = bkt
}

func (this *Stat) UpdateLastMT(bkt string, obj string, ts int64) error {
	//fmt.Printf("Stat Update %v/%v LastModifyTime: %d \n", bkt, obj, ts)
	if this.n != bkt {
		fmt.Printf("UpdateLastMT bucket inconsistent (%v , %v)\n", this.n, bkt)
	}
	// Open DB
	err := this.s.OpenStatDB(bkt)
	if err != nil {
		return err
	}
	defer this.s.CloseStatDB()

	// Update
	buf := Int64ToBytes(ts)
	err = this.s.Put(&obj, buf)

	return err
}

func (this *Stat) GetLastMT(bkt string, obj string) (int64, error) {
	if this.n != bkt {
		fmt.Printf("GetLastMT bucket inconsistent (%v , %v)\n", this.n, bkt)
	}
	var ts int64
	ts = 0
	// Open Db
	err := this.s.OpenStatDB(bkt)
	if err != nil {
		return ts, err
	}
	defer this.s.CloseStatDB()

	// Get Value
	buf := make([]byte, 20)
	err = this.s.Get(&obj, buf)
	if err == nil {
		ts = BytesToInt64(buf)
	}
	return ts, nil
}

type StatDB interface {
	OpenStatDB(dbname string) error
	CloseStatDB()
	Put(key *string, value []byte) error
	Get(key *string, value []byte) error
}

type StatRocksDB struct {
	db *gorocksdb.DB
	l  sync.Mutex
}

func (this *StatRocksDB) OpenStatDB(dbname string) error {
	var err error
	opts := gorocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	opts.SetCompression(gorocksdb.NoCompression)
	db_path := "dbs/" + dbname
	this.l.Lock()
	this.db, err = gorocksdb.OpenDb(opts, db_path)
	if err != nil {
		fmt.Printf("StatRocksDB OpenDb %v error %v\n", db_path, err)
	}
	return err
}

func (this *StatRocksDB) CloseStatDB() {
	this.db.Close()
	this.l.Unlock()
}

func (this *StatRocksDB) Put(key *string, value []byte) error {
	wopt := gorocksdb.NewDefaultWriteOptions()
	err := this.db.Put(wopt, []byte(*key), value)
	if err != nil {
		fmt.Printf("StatRocksDB Put key: %v , error : %v \n", key, err)
	}
	//fmt.Printf("StatRocksDB Put key: %v, value: %v\n", *key, value)
	return err
}

func (this *StatRocksDB) Get(key *string, value []byte) error {
	ropt := gorocksdb.NewDefaultReadOptions()
	//ropt.SetFillCache(false)
	val, err := this.db.Get(ropt, []byte(*key))
	if err != nil {
		fmt.Printf("StatRocksDB Get key: %v , error : %v \n", key, err)
	}
	defer val.Free()
	if val != nil && val.Exists() {
		copy(value, val.Data())
		//fmt.Printf("StatRocksDB Get key: %v, value: %v\n", *key, value)
	}
	return err
}
