package diskqueue

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"math/rand"
	"os"
	"path"
	"path/filepath"
	"sync"
	"time"
)

// logging stuff copied from github.com/nsqio/nsq/internal/lg

type LogLevel int

const (
	DEBUG           = LogLevel(1)
	INFO            = LogLevel(2)
	WARN            = LogLevel(3)
	ERROR           = LogLevel(4)
	FATAL           = LogLevel(5)
	numFileMsgBytes = 8
)

type AppLogFunc func(lvl LogLevel, f string, args ...interface{})

func (l LogLevel) String() string {
	switch l {
	case 1:
		return "DEBUG"
	case 2:
		return "INFO"
	case 3:
		return "WARNING"
	case 4:
		return "ERROR"
	case 5:
		return "FATAL"
	}
	panic("invalid LogLevel")
}

type Interface interface {
	Put([]byte) error
	ReadChan() <-chan []byte // this is expected to be an *unbuffered* channel
	Close() error
	Delete() error
	Depth() int64
	Empty() error
}

// diskQueue implements a filesystem backed FIFO queue
type diskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// run-time state (also persisted to disk)
	readPos       int64
	writePos      int64
	readFileNum   int64
	writeFileNum  int64
	readMessages  int64
	writeMessages int64
	writeBytes    int64
	depth         int64

	sync.RWMutex

	// instantiation time metadata
	name                string
	dataPath            string
	maxBytesDiskSpace   int64
	maxBytesPerFile     int64 // cannot change once created
	maxBytesPerFileRead int64
	minMsgSize          int32
	maxMsgSize          int32
	syncEvery           int64         // number of writes per fsync
	syncTimeout         time.Duration // duration of time per fsync
	exitFlag            int32
	needSync            bool
	badBytes            int64

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos     int64
	nextReadFileNum int64

	// keep track of the msg size we have read
	// (but not yet sent over readChan)
	readMsgSize int32

	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	writeBuf  bytes.Buffer

	// exposed via ReadChan()
	readChan chan []byte

	// internal channels
	depthChan         chan int64
	writeChan         chan []byte
	writeResponseChan chan error
	emptyChan         chan int
	emptyResponseChan chan error
	exitChan          chan int
	exitSyncChan      chan int

	logf AppLogFunc

	// disk limit implementation flag
	diskLimitFeatIsOn bool
}

// New instantiates an instance of diskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func New(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) Interface {

	return NewWithDiskSpace(name, dataPath,
		0, maxBytesPerFile,
		minMsgSize, maxMsgSize,
		syncEvery, syncTimeout, logf)
}

// Another constructor that allows users to use Disk Space Limit feature
// If user is not using Disk Space Limit feature, maxBytesDiskSpace will
// be 0
func NewWithDiskSpace(name string, dataPath string,
	maxBytesDiskSpace int64, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration, logf AppLogFunc) Interface {
	diskLimitFeatIsOn := true
	if maxBytesDiskSpace <= 0 {
		maxBytesDiskSpace = 0
		diskLimitFeatIsOn = false
	}
	d := diskQueue{
		name:              name,
		dataPath:          dataPath,
		maxBytesDiskSpace: maxBytesDiskSpace,
		maxBytesPerFile:   maxBytesPerFile,
		minMsgSize:        minMsgSize,
		maxMsgSize:        maxMsgSize,
		readChan:          make(chan []byte),
		depthChan:         make(chan int64),
		writeChan:         make(chan []byte),
		writeResponseChan: make(chan error),
		emptyChan:         make(chan int),
		emptyResponseChan: make(chan error),
		exitChan:          make(chan int),
		exitSyncChan:      make(chan int),
		syncEvery:         syncEvery,
		syncTimeout:       syncTimeout,
		logf:              logf,
		diskLimitFeatIsOn: diskLimitFeatIsOn,
	}

	d.start()
	return &d
}

// Get the last known state of DiskQueue from metadata and start ioLoop
func (d *diskQueue) start() {
	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	// get the size of all the .bad files
	badFileInfos := d.getAllBadFileInfo()
	for _, badFileInfo := range badFileInfos {
		d.badBytes += badFileInfo.Size()
	}

	go d.ioLoop()
}

// Depth returns the depth of the queue
func (d *diskQueue) Depth() int64 {
	depth, ok := <-d.depthChan
	if !ok {
		// ioLoop exited
		depth = d.depth
	}
	return depth
}

// ReadChan returns the receive-only []byte channel for reading data
func (d *diskQueue) ReadChan() <-chan []byte {
	return d.readChan
}

// Put writes a []byte to the queue
func (d *diskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.writeChan <- data
	return <-d.writeResponseChan
}

// Close cleans up the queue and persists metadata
func (d *diskQueue) Close() error {
	err := d.exit(false)
	if err != nil {
		return err
	}
	return d.sync()
}

func (d *diskQueue) Delete() error {
	return d.exit(true)
}

func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	d.exitFlag = 1

	if deleted {
		d.logf(INFO, "DISKQUEUE(%s): deleting", d.name)
	} else {
		d.logf(INFO, "DISKQUEUE(%s): closing", d.name)
	}

	close(d.exitChan)
	// ensure that ioLoop has exited
	<-d.exitSyncChan

	close(d.depthChan)

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.logf(INFO, "DISKQUEUE(%s): emptying", d.name)

	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

func (d *diskQueue) deleteAllFiles() error {
	err := d.skipToNextRWFile()

	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		d.logf(ERROR, "DISKQUEUE(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

func (d *diskQueue) skipToNextRWFile() error {
	var err error

	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		fn := d.fileName(i)
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf(ERROR, "DISKQUEUE(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	d.depth = 0

	if d.diskLimitFeatIsOn {
		d.writeBytes = 0
		d.readMessages = 0
		d.writeMessages = 0
	}

	return err
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *diskQueue) readOne() ([]byte, error) {
	var err error

	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		d.logf(INFO, "DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		// for "complete" files (i.e. not the "current" file), maxBytesPerFileRead
		// should be initialized to the file's size, or default to maxBytesPerFile
		d.maxBytesPerFileRead = d.maxBytesPerFile
		if d.readFileNum < d.writeFileNum {
			stat, err := d.readFile.Stat()
			if err == nil {
				d.maxBytesPerFileRead = stat.Size()
				if d.diskLimitFeatIsOn {
					// last 8 bytes are reserved for the number of messages in this file
					d.maxBytesPerFileRead -= numFileMsgBytes
				}
			}
		}

		d.reader = bufio.NewReader(d.readFile)
	}

	err = binary.Read(d.reader, binary.BigEndian, &d.readMsgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	if d.readMsgSize < d.minMsgSize || d.readMsgSize > d.maxMsgSize {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", d.readMsgSize)
	}

	readBuf := make([]byte, d.readMsgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + d.readMsgSize)

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// we only consider rotating if we're reading a "complete" file
	// and since we cannot know the size at which it was rotated, we
	// rely on maxBytesPerFileRead rather than maxBytesPerFile
	if d.readFileNum < d.writeFileNum && d.nextReadPos >= d.maxBytesPerFileRead {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}

	return readBuf, nil
}

// get the size of the metaData file or its max possible size
func (d *diskQueue) metaDataFileSize() int64 {
	var err error
	var metaDataFile *os.File

	metaDataFile, err = os.OpenFile(d.metaDataFileName(), os.O_RDONLY, 0600)

	var metaDataFileSize int64
	if err == nil {
		var stat os.FileInfo

		stat, err = metaDataFile.Stat()
		if err == nil {
			metaDataFileSize = stat.Size()
		}
	}
	if err != nil {
		// use max file size (8 int64 fields)
		metaDataFileSize = 64
	}

	return metaDataFileSize
}

func (d *diskQueue) removeReadFile() error {
	var err error

	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return err
		}
	}

	closeReadFile := func() {
		d.readFile.Close()
		d.readFile = nil
	}
	defer closeReadFile()

	// read total messages number at the end of the file
	_, err = d.readFile.Seek(-numFileMsgBytes, 2)
	if err != nil {
		return err
	}

	var totalMessages int64
	err = binary.Read(d.reader, binary.BigEndian, &totalMessages)
	if err != nil {
		return err
	}

	// update depth with the remaining number of messages
	d.depth -= totalMessages - d.readMessages

	// get the size of the file
	stat, err := d.readFile.Stat()
	if err != nil {
		return err
	}
	readFileSize := stat.Size()

	// we have not finished reading this file
	if d.readFileNum == d.nextReadFileNum {
		d.nextReadFileNum++
		d.nextReadPos = 0
	}

	d.moveToNextReadFile(readFileSize)

	return nil
}

func (d *diskQueue) getAllBadFileInfo() []fs.FileInfo {
	var badFileInfos []fs.FileInfo

	getBadFileInfos := func(path string, d fs.DirEntry, err error) error {
		if d.IsDir() {
			// if the entry is a directory, skip it
			return fs.SkipDir
		}

		if err != nil {
			return err
		}

		if filepath.Ext(d.Name()) == ".bad" {
			badFileInfo, e := d.Info()
			if e != nil && badFileInfo != nil {
				badFileInfos = append(badFileInfos, badFileInfo)
			}
		}

		return nil
	}

	err := filepath.WalkDir(d.dataPath, getBadFileInfos)
	if err != nil {
		return nil
	}

	return badFileInfos
}

func (d *diskQueue) freeUpDiskSpace() error {
	var err error
	badFileExists := false

	// delete .bad files before deleting non-corrupted files (i.e. readFile)
	if d.badBytes != 0 {
		badFileInfos := d.getAllBadFileInfo()

		// if badBytes is negative, something went wrong, so recalculate total .bad files size
		if d.badBytes < 0 {
			d.badBytes = 0
			for _, badFileInfo := range badFileInfos {
				d.badBytes += badFileInfo.Size()
			}
		}

		// check if a .bad file exists. If it does, delete that first
		if badFileInfos != nil {
			badFileExists = true

			oldestBadFileInfo := badFileInfos[0]
			badFileFilePath := path.Join(d.dataPath, oldestBadFileInfo.Name())

			err = os.Remove(badFileFilePath)
			if err == nil {
				d.badBytes -= oldestBadFileInfo.Size()
			} else {
				d.logf(ERROR, "DISKQUEUE(%s) failed to remove .bad file(%s) - %s", d.name, oldestBadFileInfo.Name(), err)
			}
		} else {
			// there are no bad files
			d.badBytes = 0
		}
	}

	if !badFileExists {
		// delete the read file (make space)
		if d.readFileNum == d.writeFileNum {
			d.skipToNextRWFile()
		} else {
			err = d.removeReadFile()
			if err != nil {
				d.logf(ERROR, "DISKQUEUE(%s) not able to delete file(%s) - %s",
					d.name, d.fileName(d.readFileNum), err)
				d.handleReadError()
				return err
			}
		}
	}

	return nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *diskQueue) writeOne(data []byte) error {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		d.logf(INFO, "DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	dataLen := int32(len(data))

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) minMsgSize=%d maxMsgSize=%d", dataLen, d.minMsgSize, d.maxMsgSize)
	}

	totalBytes := int64(4 + dataLen)
	reachedFileSizeLimit := false

	if d.diskLimitFeatIsOn {
		// If there the data to be written is bigger than the disk size limit, do not write
		if totalBytes+8 > d.maxBytesDiskSpace {
			return errors.New("Not enough disk space to write message")
		}

		// check if we have enough space to write this message
		metaDataFileSize := d.metaDataFileSize()

		// check if we will reach or surpass file size limit
		if d.writePos+totalBytes+numFileMsgBytes >= d.maxBytesPerFile {
			reachedFileSizeLimit = true
		}

		expectedBytesIncrease := totalBytes
		if reachedFileSizeLimit {
			expectedBytesIncrease += numFileMsgBytes
		}

		// keep freeing up disk space until we have enough space to write this message
		for d.badBytes+metaDataFileSize+d.writeBytes+expectedBytesIncrease > d.maxBytesDiskSpace {
			d.freeUpDiskSpace()
		}
	} else if d.writePos+totalBytes >= d.maxBytesPerFile {
		reachedFileSizeLimit = true
	}

	// add all data to writeBuf before writing to file
	// this causes everything to be written to file or nothing
	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	// check if we reached the file size limit with this message
	if d.diskLimitFeatIsOn && reachedFileSizeLimit {
		// write number of messages in binary to file
		err = binary.Write(&d.writeBuf, binary.BigEndian, d.writeMessages+1)
		if err != nil {
			return err
		}
	}

	// only write to the file once
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	d.writePos += totalBytes
	d.depth += 1

	if d.diskLimitFeatIsOn {
		d.writeBytes += totalBytes
		d.writeMessages += 1
	}

	if reachedFileSizeLimit {
		if d.readFileNum == d.writeFileNum {
			d.maxBytesPerFileRead = d.writePos
		}

		d.writeFileNum++
		d.writePos = 0

		if d.diskLimitFeatIsOn {
			// add bytes for the number of messages in the file
			d.writeBytes += numFileMsgBytes
			d.writeMessages = 0
		}

		// sync every time we start writing to a new file
		err = d.sync()
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return err
}

// sync fsyncs the current writeFile and persists metadata
func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// retrieveMetaData initializes state from the filesystem
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	// if user is using disk space limit feature
	if d.diskLimitFeatIsOn {
		_, err = fmt.Fscanf(f, "%d\n%d,%d,%d\n%d,%d,%d,%d\n",
			&d.depth,
			&d.readFileNum, &d.readMessages, &d.readPos,
			&d.writeBytes, &d.writeFileNum, &d.writeMessages, &d.writePos)
	} else {
		_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
			&d.depth,
			&d.readFileNum, &d.readPos,
			&d.writeFileNum, &d.writePos)
	}

	if err != nil {
		return err
	}

	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	// if user is using disk space limit feature
	if d.diskLimitFeatIsOn {
		_, err = fmt.Fprintf(f, "%d\n%d,%d,%d\n%d,%d,%d,%d\n",
			d.depth,
			d.readFileNum, d.readMessages, d.readPos,
			d.writeBytes, d.writeFileNum, d.writeMessages, d.writePos)
	} else {
		_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
			d.depth,
			d.readFileNum, d.readPos,
			d.writeFileNum, d.writePos)
	}
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	// atomically rename
	return os.Rename(tmpFileName, fileName)
}

func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

func (d *diskQueue) checkTailCorruption(depth int64) {
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}

	// we've reached the end of the diskqueue
	// if depth isn't 0 something went wrong
	if depth != 0 {
		if depth < 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) negative depth at tail (%d), metadata corruption, resetting 0...",
				d.name, depth)
		} else if depth > 0 {
			d.logf(ERROR,
				"DISKQUEUE(%s) positive depth at tail (%d), data loss, resetting 0...",
				d.name, depth)
		}
		// force set depth 0
		d.depth = 0
		d.needSync = true
	}

	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			d.logf(ERROR,
				"DISKQUEUE(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.writeFileNum)
		}

		if d.readPos > d.writePos {
			d.logf(ERROR,
				"DISKQUEUE(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}

		d.skipToNextRWFile()
		d.needSync = true
	}
}

func (d *diskQueue) moveToNextReadFile(readFileSize int64) {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos

	// see if we need to clean up the old file
	if oldReadFileNum != d.nextReadFileNum {

		// sync every time we start reading from a new file
		d.needSync = true

		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			d.logf(ERROR, "DISKQUEUE(%s) failed to Remove(%s) - %s", d.name, fn, err)
		}

		if d.diskLimitFeatIsOn {
			d.readMessages = 0
			d.writeBytes -= readFileSize
		}
	}
}

func (d *diskQueue) moveForward() {
	// add bytes for the number of messages and the size of the message
	readFileSize := int64(d.readMsgSize) + d.readPos + 12
	d.depth -= 1

	if d.diskLimitFeatIsOn {
		d.readMessages += 1
	}

	d.moveToNextReadFile(readFileSize)

	d.checkTailCorruption(d.depth)
}

func (d *diskQueue) handleReadError() {
	// jump to the next read file and rename the current (bad) file
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
	}

	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	d.logf(WARN,
		"DISKQUEUE(%s) jump to next file and saving bad file as %s",
		d.name, badRenameFn)

	err := os.Rename(badFn, badRenameFn)
	if err != nil {
		d.logf(ERROR,
			"DISKQUEUE(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}

	if d.diskLimitFeatIsOn {
		d.badBytes += d.maxBytesPerFileRead
		if d.maxBytesPerFileRead == d.maxBytesPerFile {
			// this could mean that we were not able to get the
			// correct file size
			d.badBytes += int64(d.maxMsgSize) + 4
		}
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0

	// significant state change, schedule a sync on the next iteration
	d.needSync = true
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte

	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// dont sync all the time :)
		if count == d.syncEvery {
			d.needSync = true
		}

		if d.needSync {
			err = d.sync()
			if err != nil {
				d.logf(ERROR, "DISKQUEUE(%s) failed to sync - %s", d.name, err)
			}
			count = 0
		}

		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					d.logf(ERROR, "DISKQUEUE(%s) reading at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			r = d.readChan
		} else {
			r = nil
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		case r <- dataRead:
			count++
			// moveForward sets needSync flag if a file is removed
			d.moveForward()
		case d.depthChan <- d.depth:
		case <-d.emptyChan:
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		case dataWrite := <-d.writeChan:
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
		case <-syncTicker.C:
			if count == 0 {
				// avoid sync when there's no activity
				continue
			}
			d.needSync = true
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	d.logf(INFO, "DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
