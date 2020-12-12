const path = require('path')
const fs = require('fs').promises
const fileSystem = require('fs')
const Keyify = require('keyify')
const { Recorder, Player } = require('transcript')
const { Readable } = require('stream')
const Interrupt = require('interrupt')
const assert = require('assert')
const coalesce = require('extant')

let latch

class WriteAhead {
    static Error = Interrupt.create('WriteAhead.Error', {
        'IO_ERROR': {},
        'OPEN_ERROR': {
            code: 'IO_ERROR',
            message: 'unable to open file'
        },
        'BLOCK_SHORT_READ': {
            code: 'IO_ERROR',
            message: 'incomplete read of block'
        },
        'BLOCK_MISSING': {
            code: 'IO_ERROR',
            message: 'block did not parse correctly'
        },
        'INVALID_CHECKSUM': {
            code: 'IO_ERROR',
            message: 'block contained an invalid checksum'
        }
    })

    static Stream = class extends Readable {
        constructor (writeahead, key, poker = null) {
            super()
            this.writeahead = writeahead
            this._pokers = { $pokers: poker == null ? [] : [ poker ] }
            this._index = 0
            this._keyified = Keyify.stringify(key)
            this.key = key
            this._blocks = []
            this._logs = this.writeahead._logs.slice()
            this._remainder = Buffer.alloc(0)
            this._player = new Player(this.writeahead._checksum)
            this._log = null
            this._filename = null
        }

        _calledback (error, ...vargs) {
            const continued = vargs.pop()
            if (error != null) {
                this.destroy(error)
            } else {
                try {
                    continued.apply(this, vargs)
                } catch (error) {
                    this.destroy(error)
                }
            }
        }

        __read (options, poker) {
            const merged = WriteAhead.Error.options(options, { $pokers: poker })
            if (this._blocks.length == 0) {
                if (this._fd != null) {
                    fileSystem.close(this._fd, WriteAhead.Error.callback(merged, 'CLOSE_ERROR', { filename: this._filename }, $ => $(), (error, options) => {
                        this._calledback(error, () => {
                            this._fd = null
                            this.__read(options, $ => $())
                        })
                    }))
                } else {
                    if (this._index == this.writeahead._logs.length) {
                        this.push(null)
                    } else {
                        this._log = this.writeahead._logs[this._index++]
                        this._blocks = this.writeahead._blocks[this._log.id][this._keyified].slice()
                        this.__read(merged, $ => $())
                    }
                }
            } else if (this._fd == null) {
                this._filename = path.join(this.writeahead.directory, String(this._log.id))
                fileSystem.open(this._filename, 'r', WriteAhead.Error.callback(merged, 'OPEN_ERROR', { filename: this._filename }, $ => $(), (error, fd, options) => {
                    this._calledback(error, () => {
                        this._fd = fd
                        this.__read(options, $ => $())
                    })
                }))
            } else {
                const block = this._blocks.shift()
                const buffer = Buffer.alloc(block.length)
                fileSystem.read(this._fd, buffer, 0, buffer.length, block.position, WriteAhead.Error.callback(merged, 'READ_ERROR', { filename: this._filename }, $ => $(), (error, read) => {
                    this._calledback(error, () => {
                        WriteAhead.Error.assert(read == buffer.length, 'BLOCK_SHORT_READ')
                        const entries = this._player.split(buffer)
                        WriteAhead.Error.assert(entries.length == 1, 'BLOCK_MISSING')
                        this.push(entries[0].parts[1])
                    })
                }))
            }
        }

        _read () {
            this.__read(this._pokers, $ => $())
        }

        _destroy (error, callback) {
            if (this._fd != null) {
                fileSystem.close(this._fd, e => {
                    this._fd = null
                    this._destroy(e || error, callback)
                })
            } else if (error) {
                callback(error)
            } else {
                callback()
            }
        }
    }

    constructor ({ directory, logs, checksum, blocks }) {
        this.directory = directory
        this._logs = logs
        this._recorder = Recorder.create(checksum)
        this._blocks = blocks
        this._checksum = checksum
        if (this._logs.length == 0) {
            this._blocks[0] = []
            this._logs.push({ id: 0, readers: 0, shifted: false })
        }
    }

    static async open ({ directory, checksum = () => 0 }, converter) {
        const dir = await fs.readdir(directory)
        const ids = dir.filter(file => /^\d+$/.test(file))
                       .map(log => +log)
                       .sort((left, right) => left - right)
        const player = new Player(checksum)
        const blocks = {}
        for (const id of ids) {
            blocks[id] = {}
            const readable = fileSystem.createReadStream(path.join(directory, String(id)))
            let position = 0, remainder = 0
            for await (const block of readable) {
                let offset = 0
                for (;;) {
                    const [ entry ] = player.split(block.slice(offset), 1)
                    if (entry == null) {
                        break
                    }
                    const keys = JSON.parse(String(entry.parts[0]))
                                     .map(key => Keyify.stringify(key))
                    const length = entry.sizes.reduce((sum, value) => sum + value, 0)
                    for (const key of keys) {
                        blocks[id][key] || (blocks[id][key] = [])
                        blocks[id][key].push({ position, length })
                    }
                    position += length
                    offset += length - remainder
                    remainder = 0
                }
                remainder = block.length - offset
            }
        }
        const logs = ids.map(id => ({ id, readers: 0, shifted: false }))
        return new WriteAhead({ directory, logs, checksum, blocks })
    }

    // Write a batch of entries to the write-ahead log. `entries` is an array of
    // application specific objects to log. `converter` converts the entry to a
    // set of keys, header and body.
    //
    // The keys are used to reconstruct a file stream from the write-ahead log.
    // The body of the message will be included in the stream constructed for
    // any of the keys in the set of keys. It is up to the application to fish
    // out the relevant content from the body for a given key.

    //
    async write (entries, converter) {
        const { _recorder: recorder } = this
        const log = this._logs[this._logs.length - 1]
        const filename = path.join(this.directory, String(log.id))
        const handle = await fs.open(filename, 'a')
        try {
            const stat = await handle.stat()
            let position = stat.size
            const buffers = [], positions = {}
            for (const entry of entries) {
                const { keys, body } = converter(entry)
                const keyified = keys.map(key => Keyify.stringify(key))
                const block = recorder([[ Buffer.from(JSON.stringify(keys)) ], [ body ]])
                for (const key of keyified) {
                    positions[key] || (positions[key] = [])
                    positions[key].push({ position, length: block.length })
                }
                position += block.length
                buffers.push(block)
            }
            await handle.appendFile(Buffer.concat(buffers))
            await handle.sync()
            for (const key in positions) {
                this._blocks[log.id][key] || (this._blocks[log.id][key] = [])
                this._blocks[log.id][key].push.apply(this._blocks[log.id][key], positions[key])
            }
        } finally {
            await handle.close()
        }
    }

    async rotate () {
        const log = { id: this._logs[this._logs.length - 1].id + 1, readers: 0, shifted: false }
        const filename = path.join(this.directory, String(log.id))
        await fs.writeFile(filename, '', { flags: 'wx' })
        this._logs.push(log)
        this._blocks[log.id] = []
    }

    async shift () {
        if (this._logs.length == 0) {
            return false
        }
        /*
        if (this._logs[0].readers != 0) {
            this._logs[0].latch = {
                promise: new Promise(resolve => latch = { resolve }),
                ...latch
            }
            await this._logs[0].latch.promise
        }
        */
        const log = this._logs.shift()
        const filename = path.join(this.directory, String(log.id))
        await fs.unlink(filename)
        return true
    }
}

module.exports = WriteAhead
