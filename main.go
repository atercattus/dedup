package main

import (
	"context"
	"crypto/sha256"
	"flag"
	"fmt"
	"github.com/pkg/errors"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"hash"
	"io"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"
)

type (
	FileSize int64
	Task     struct {
		name  string
		isDir bool
		size  FileSize
	}
)

var (
	argv struct {
		help      bool
		workers   uint
		dbRoot    string
		filesRoot string

		followSymlinks bool
		ignoreErrors   bool
	}

	db *leveldb.DB

	processedCnt int64

	cancelFunc context.CancelFunc
	lastError  error
)

func init() {
	flag.BoolVar(&argv.help, `h`, false, `show this help`)
	flag.UintVar(&argv.workers, `workers`, 0, `workers count (CPU count by default)`)
	flag.StringVar(&argv.filesRoot, `root`, ``, `root files directory`)
	flag.StringVar(&argv.dbRoot, `db`, `db`, `database directory`)
	flag.BoolVar(&argv.followSymlinks, `follow-symlinks`, false, `follow symlinks`)
	flag.BoolVar(&argv.ignoreErrors, `ignore-errors`, false, `ignore error when reading files`)
	flag.Parse()
}

func main() {
	if argv.help {
		flag.PrintDefaults()
		return
	}

	var err error

	dbOpt := &opt.Options{
		//Filter: filter.NewBloomFilter(10),
		CompactionTableSize: 16 * 1024 * 1024,
		WriteBuffer:         16 * 1024 * 1024,
	}
	db, err = leveldb.OpenFile(argv.dbRoot, dbOpt)
	if err != nil {
		log.Panicln(err)
	}
	defer func() {
		db.Close()
	}()

	if true {
		log.Println(`Clean DB`)
		iter := db.NewIterator(nil, nil)
		for iter.Next() {
			db.Delete(iter.Key(), nil)
		}
		iter.Release()
		if err = iter.Error(); err != nil {
			log.Panicln(err)
		}
		db.CompactRange(util.Range{nil, nil})
		//return
	}

	if argv.workers == 0 {
		argv.workers = uint(runtime.NumCPU())
	}

	var ctx context.Context
	ctx, cancelFunc = context.WithCancel(context.Background())
	var wg sync.WaitGroup
	tasks := make(chan Task, argv.workers)

	for worker := uint(0); worker < argv.workers; worker++ {
		wg.Add(1)
		go func() {
			workerFunc(ctx, tasks)
			wg.Done()
		}()
	}

	go func() {
		for range time.Tick(5 * time.Second) {
			fmt.Printf("Cur processed: %d\n", atomic.LoadInt64(&processedCnt))
		}
	}()

	log.Println(`Start!`)

	err = filepath.Walk(argv.filesRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			//log.Println(`walk error:`, err) // permission denied
			return nil
		} else if info == nil {
			return nil
		} else if lastError != nil {
			return lastError
		}

		mode := info.Mode()

		isDir := mode&os.ModeDir != 0
		isSymlink := mode&os.ModeSymlink != 0

		if isSymlink && !argv.followSymlinks {
			return nil
		}

		task := Task{
			name:  path,
			isDir: isDir,
			size:  FileSize(info.Size()),
		}

		select {
		case tasks <- task:
		case <-ctx.Done():
		}

		return err
	})

	cancelFunc()

	if err != nil {
		log.Println(err)
	}

	wg.Wait()

	fmt.Printf("Total processed: %d\n", atomic.LoadInt64(&processedCnt))

	os.Stdout.Sync()
}

func workerFunc(ctx context.Context, tasks chan Task) {
	var (
		hasher    = sha256.New()
		hasherSum = make([]byte, hasher.BlockSize()+int(unsafe.Sizeof(FileSize(0)))) // + для размера файла
	)

loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case task, ok := <-tasks:
			if !ok {
				break loop
			}

			var err error
			if !task.isDir {
				err = processFile(task.name, task.size, hasher, hasherSum)
			}
			if err != nil && !argv.ignoreErrors {
				lastError = errors.Wrap(err, `process file fail`)
				cancelFunc()
				break loop
			}
		}
	}
}

func processFile(path string, size FileSize, hasher hash.Hash, hasherBuf []byte) error {
	hasher.Reset()

	fd, err := os.Open(path)
	if err != nil {
		return errors.Wrap(err, `cannot open file`)
	}
	defer fd.Close()

	if _, err := io.Copy(hasher, fd); err != nil {
		return errors.Wrap(err, `cannot hashing file`)
	}

	hasherBuf = hasher.Sum(hasherBuf[:0])
	hasherBuf = strconv.AppendUint(hasherBuf, uint64(size), 10)

	err = db.Put(hasherBuf, []byte(path), nil)
	if err != nil {
		return errors.Wrap(err, `cannot write to DB`)
	}

	atomic.AddInt64(&processedCnt, 1)

	return nil
}
