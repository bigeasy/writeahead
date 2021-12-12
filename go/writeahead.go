package writeahead

import (
    transcript "github.com/bigeasy/transcript/go"
    "path"
    "sort"
    "strconv"
    "regexp"
    "io/ioutil"
    "sync"
    "encoding/json"
    "os"
    "fmt"
)

type Write struct {
    keys []string
    buffer []byte
}

type _Write struct {
    keys []string
    buffer []byte
    sync bool
}

type _Block struct {
    next *_Block
    id uint64
    position int64
    keys []string
    buffer []byte
}

type _BlockList struct {
    length int
    head *_Block
    tail *_Block
}

type _BlockWait struct {
    next *_BlockWait
    position chan int64
    block *_Block
}

type _BlockWrite struct {
    keys []string
    buffer []byte
    position chan int64
}

type _Log struct {
    id int
    shifted bool
    blocks map[string][]_Block
    _blocks map[string]*_BlockList
}

type _Appender struct {
    file *os.File
    position int64
    recorder *transcript.Recorder
}

type WriteAhead struct {
    directory string
    checksum transcript.ChecksumFunc
    writes int
    logs []_Log
    writer chan _BlockWrite
    head *_BlockWait
    last *_BlockWait
    lock *sync.RWMutex
    appender *_Appender
    requests chan _Request
}

// https://stackoverflow.com/questions/3398490/checking-if-a-channel-has-a-ready-to-read-value-using-go
// https://stackoverflow.com/questions/33328000/reading-multiple-elements-from-a-channel-in-go
func _WriteAhead (appender *_Appender, writes <-chan _BlockWrite) {
    blockWrites := make([]_BlockWrite, 0, 32)
    for first := range writes {
        blockWrites = append(blockWrites, first)
        GATHER: for len(blockWrites) != cap(blockWrites) {
            select {
            case next := <-writes:
                blockWrites = append(blockWrites, next)
            default:
                break GATHER
            }
        }
        buffers := make([][]byte, len(blockWrites))
        positions := make([]int64, len(blockWrites))
        for i, block := range blockWrites {
            keys, _ := json.Marshal(block.keys)
            buffers[i] = appender.recorder.Record([][][]byte{ [][]byte{ keys, block.buffer } })
            positions[i] = appender.position
            appender.position += int64(len(buffers[i]))
    //        block := _Block{write.keys, buffers[i], writeahead.position}
        }
        //bytes, err := vectorio.Writev(f, buffers)
    }
}

func _ClearWrites (writeahead *WriteAhead) {
    for writeahead.writes != 0 {
    }
}

type _Request struct {
    method string
    message interface{}
}

type _GetRequest struct {
    key string
}

type _WriteRequest struct {
    keys []string
    buffer []byte
    enqueued chan bool
    written chan bool
}

func Coordinator (writeahead WriteAhead, requests <-chan _Request) {
    for request := range requests {
        switch request.message {
        case "get": {
                get := request.message.(_GetRequest)
                writeahead._Get(get.key)
            }
        case "write": {
                writeahead._Write(request.message.(_WriteRequest))
            }
        case "rotate": {
            }
        case "shift": {
            }
        }
    }
}

func NewWriteAhead (directory string, checksum transcript.ChecksumFunc) (*WriteAhead, error) {
    files, err := ioutil.ReadDir(directory)
    if err != nil {
        return nil, err
    }
    logs := make([]_Log, 0, 16)
    digits, _ := regexp.Compile("^\\d+$")
    fmt.Fprintf(os.Stdout, "here %d \n", len(files))
    ids := make([]int, 0, len(files))
    for _, file := range files {
        if ! digits.MatchString(file.Name()) {
            continue
        }
        id, _ := strconv.Atoi(file.Name())
        ids = append(ids, id)
    }
    sort.Ints(ids)
    if len(logs) == 0 {
        logs = append(logs, _Log{0, false, make(map[string][]_Block), make(map[string]*_BlockList)})
    }
    appender := &_Appender{ nil, 0, transcript.NewRecorder(checksum) }
    wait := &_BlockWait{}
    writeahead := WriteAhead{
        directory: directory,
        checksum: checksum,
        logs: logs,
        writer: make(chan _BlockWrite),
        head: wait,
        last: wait,
        lock: new(sync.RWMutex),
        appender: appender,
        requests: make(chan _Request),
    }
    log := logs[len(logs) - 1]
    filename := path.Join(directory, strconv.Itoa(log.id))
    file, err := os.OpenFile(filename, os.O_CREATE | os.O_APPEND, 0644)
    if err != nil {
        return nil, err
    }
    stat, err := file.Stat()
    if err != nil {
        return nil, err
    }
    writeahead.appender.position = stat.Size()
    writeahead.appender.file = file
    fmt.Fprintf(os.Stdout, "position %d \n", writeahead.appender.position)
    return &writeahead, nil
}

func (writeahead *WriteAhead) Write (keys []string, buffer []byte) chan bool {
    write := _WriteRequest{keys, buffer, make(chan bool, 1), make(chan bool, 1)}
    writeahead.requests <- _Request{"write", write}
    <-write.enqueued
    return write.written
}

func (writeahead *WriteAhead) _Write (write _WriteRequest) {
    log := writeahead.logs[len(writeahead.logs) - 1]
    block := _Block{nil, 0, 0, write.keys, write.buffer}
    for _, key := range write.keys {
        var list *_BlockList
        if log._blocks[key] == nil {
            list = &_BlockList{}
            log._blocks[key] = list
            list.length = 1
            list.head = &block
            list.tail = &block
        } else {
            list = log._blocks[key]
            list.length++
            list.tail.next = &block
            list.tail = &block
        }
    }
    position := make(chan int64, 1)
    blockWait := _BlockWait{position: position, block: &block}
    writeahead.last.next = &blockWait
    writeahead.last = &blockWait
    writeahead.writes++
    writeahead.writer <- _BlockWrite{write.keys, write.buffer, position}
    write.enqueued <-true
}

func (writeahead *WriteAhead) _Get (key string) <-chan []byte {
    //player := transcript.NewPlayer(writeahead.checksum)
    // Could put RWLock on each log, I think, but this is easier to understand.
    writeahead.lock.RLock()
    defer writeahead.lock.RUnlock()
    //for _, log := range logs {
    //}
    return nil
}

func (writeahead *WriteAhead) Rotate () {
    log := _Log{0, false, make(map[string][]_Block), make(map[string]*_BlockList)}
    writeahead.logs = append(writeahead.logs, log)
}

func (writeahead *WriteAhead) Shift () {
    if len(writeahead.logs) != 0 {
        log := writeahead.logs[0]
        writeahead.logs = writeahead.logs[1:]
        log.shifted = true
    }
}
