package main

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/url"
	"os"
	"sort"

	"github.com/brycekahle/goamz/aws"
	"github.com/brycekahle/goamz/s3"
	consul "github.com/hashicorp/consul/api"
)

var (
	restoreMode bool
	overwrite   bool
)

func init() {
	flag.BoolVar(&restoreMode, "restore", false, "Restore data instead of backing up")
	flag.BoolVar(&overwrite, "f", false, "Overwrite existing files")
}

type ByCreateIndex consul.KVPairs

func (a ByCreateIndex) Len() int      { return len(a) }
func (a ByCreateIndex) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

//Sort the KVs by createIndex
func (a ByCreateIndex) Less(i, j int) bool { return a[i].CreateIndex < a[j].CreateIndex }

func backup(outfile string) {
	client, _ := consul.NewClient(consul.DefaultConfig())
	kv := client.KV()

	pairs, _, err := kv.List("/", nil)
	if err != nil {
		panic(err)
	}
	sort.Sort(ByCreateIndex(pairs))

	outpairs := map[string]string{}
	for _, element := range pairs {
		outpairs[element.Key] = string(element.Value)
	}

	data, err := json.MarshalIndent(outpairs, "", "  ")
	if err != nil {
		panic(err)
	}

	writeOutput(data, outfile)
}

func writeOutput(data []byte, outfile string) {
	u, err := url.Parse(outfile)
	if err != nil {
		panic(err)
	}

	if u.Scheme == "" {
		if _, err := os.Stat(outfile); !overwrite && err == nil {
			log.Fatalf("%s exists. Use -f to force overwrite", outfile)
			return
		}

		if err = ioutil.WriteFile(outfile, data, 0644); err != nil {
			panic(err)
		}
		return
	}

	if u.Scheme == "s3" {
		auth, err := aws.EnvAuth()
		if err != nil {
			panic(err)
		}

		md5bytes := md5.Sum(data)
		sum := base64.StdEncoding.EncodeToString(md5bytes[:])

		awss3 := s3.New(auth, aws.GetRegion("us-west-2"))
		bucket := awss3.Bucket(u.Host)
		version, err := bucket.Put(u.Path, data, "application/json", s3.Private, s3.Options{ContentMD5: sum})
		if err != nil {
			panic(err)
		}

		log.Printf("Backed up to %s%s version %s", u.Host, u.Path, version)
		return
	}

	log.Fatalf("Unknown scheme %s", u.Scheme)
}

func readInput(infile string) ([]byte, error) {
	u, err := url.Parse(infile)
	if err != nil {
		return nil, err
	}

	if u.Scheme == "" {
		return ioutil.ReadFile(infile)
	}

	if u.Scheme == "s3" {
		auth, err := aws.EnvAuth()
		if err != nil {
			return nil, err
		}
		awss3 := s3.New(auth, aws.GetRegion("us-west-2"))
		bucket := awss3.Bucket(u.Host)

		return bucket.Get(u.Path)
	}

	return nil, fmt.Errorf("Unknown scheme %s", u.Scheme)
}

func restore(infile string) {
	data, err := readInput(infile)
	if err != nil {
		panic(err)
	}

	inpairs := map[string]string{}
	if err = json.Unmarshal(data, &inpairs); err != nil {
		panic(err)
	}

	client, _ := consul.NewClient(consul.DefaultConfig())
	kv := client.KV()

	for k, v := range inpairs {
		log.Printf("restoring %s:%s", k, v)
		p := &consul.KVPair{Key: k, Value: []byte(v)}
		_, err := kv.Put(p, nil)
		if err != nil {
			panic(err)
		}
	}
}

func main() {
	flag.Parse()
	if len(flag.Args()) == 0 {
		flag.Usage()
		os.Exit(2)
	}

	filename := flag.Args()[0]
	if restoreMode {
		restore(filename)
	} else {
		backup(filename)
	}
}
