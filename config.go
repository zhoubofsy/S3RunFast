package main

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type RGWConfig struct {
	Endpoint string `json:"Endpoint"`
	Access   string `json:"AccessKey"`
	Secret   string `json:"SecretKey"`
}

type Config struct {
	WorkerPoolSz int       `json:"WorkerPoolSize"`
	Src          RGWConfig `json:"Source"`
	Dest         RGWConfig `json:"Destination"`
}

func (this *Config) load(f string) error {
	raw, err := ioutil.ReadFile(f)
	if err != nil {
		fmt.Println("Read file error : ", err)
		return err
	}
	err = json.Unmarshal(raw, this)
	if err != nil {
		fmt.Println("Unmarshal error : ", err)
	}
	return err
}
