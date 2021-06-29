package writeahead

import (
    transcript "github.com/bigeasy/transcript/go"
    "sync"
//    "github.com/google/vectorio"
    "encoding/json"
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
    id uint64
    position uint64
    keys []string
    buffer []byte
}

type _BlockWait struct {
    next *_BlockWait
    position chan uint64
    block *_Block
}

type _BlockWrite struct {
    keys []string
    buffer []byte
    position chan uint64
}

type _Log struct {
    id int
    shifted bool
    blocks map[string][]_Block
}

type _Appender struct {
    position uint64
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
    blocks := make([]_BlockWrite, 0, 32)
    for first := range writes {
        blocks = append(blocks, first)
        GATHER: for len(blocks) != cap(blocks) {
            select {
            case next := <-writes:
                blocks = append(blocks, next)
            default:
                break GATHER
            }
        }
        buffers := make([][]byte, len(blocks))
        positions := make([]uint64, len(blocks))
        for i, block := range blocks {
            keys, _ := json.Marshal(block.keys)
            buffers[i] = appender.recorder.Record([][][]byte{ [][]byte{ keys, block.buffer } })
            positions[i] = appender.position
    //        block := _Block{write.keys, buffers[i], writeahead.position}
        }
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

func Coordinator (writeahead WriteAhead, requests <-chan _Request) {
    for request := range requests {
        switch request.message {
        case "get": {
                get := request.message.(_GetRequest)
                writeahead._Get(get.key)
            }
        case "write": {
            }
        case "rotate": {
            }
        case "shift": {
            }
        }
    }
}

func NewWriteAhead (directory string, checksum transcript.ChecksumFunc) *WriteAhead {
    log := _Log{0, false, make(map[string][]_Block)}
    appender := &_Appender{ 0, transcript.NewRecorder(checksum) }
    wait := &_BlockWait{}
    writeahead := WriteAhead{
        directory: directory,
        checksum: checksum,
        logs: []_Log{ log },
        writer: make(chan _BlockWrite),
        head: wait,
        last: wait,
        lock: new(sync.RWMutex),
        appender: appender,
        requests: make(chan _Request),
    }
    return &writeahead
}

func (writeahead *WriteAhead) Write (writes []Write) {
}

func (writeahead *WriteAhead) _Write (writes []Write) bool {
    log := writeahead.logs[len(writeahead.logs) - 1]
    for _, write := range writes {
        block := _Block{0, 0, write.keys, write.buffer}
        for _, key := range write.keys {
            if log.blocks[key] == nil {
                log.blocks[key] = make([]_Block, 0, 32)
            }
            log.blocks[key] = append(log.blocks[key], block)
        }
        position := make(chan uint64, 1)
        blockWait := _BlockWait{position: position, block: &block}
        writeahead.last.next = &blockWait
        writeahead.last = &blockWait
        writeahead.writes++
        writeahead.writer <- _BlockWrite{write.keys, write.buffer, position}
    }
    return true
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
    log := _Log{0, false, make(map[string][]_Block)}
    writeahead.logs = append(writeahead.logs, log)
}

func (writeahead *WriteAhead) Shift () {
    if len(writeahead.logs) != 0 {
        log := writeahead.logs[0]
        writeahead.logs = writeahead.logs[1:]
        log.shifted = true
    }
}
