const path = require('path')
const fs = require('fs').promises
const fileSystem = require('fs')
const Keyify = require('keyify')
const { Recorder, Player } = require('transcript')
const { Readable } = require('stream')
const Interrupt = require('interrupt')
const assert = require('assert')
const coalesce = require('extant')
const Sequester = require('sequester')
const Staccato = { Readable: require('staccato/readable') }

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

    constructor ({ directory, logs, checksum, blocks }) {
        this.directory = directory
        this._logs = logs
        this._recorder = Recorder.create(checksum)
        this._blocks = blocks
        this._checksum = checksum
        if (this._logs.length == 0) {
            this._blocks[0] = []
            this._logs.push({
                id: 0,
                shifted: false,
                sequester: new Sequester
            })
        }
    }

    static async open ({ directory, checksum = () => 0 }) {
        const dir = await fs.readdir(directory)
        const ids = dir.filter(file => /^\d+$/.test(file))
                       .map(log => +log)
                       .sort((left, right) => left - right)
        const player = new Player(checksum)
        const blocks = {}
        for (const id of ids) {
            blocks[id] = {}
            const stream = fileSystem.createReadStream(path.join(directory, String(id)))
            const readable = new Staccato.Readable(stream)
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
        const logs = ids.map(id => ({ id, shifted: false, sequester: new Sequester }))
        return new WriteAhead({ directory, logs, checksum, blocks })
    }
    //

    // Returns an asynchronous iterator over the blocks for the given key.

    //
    async *get (key) {
        const keyified = Keyify.stringify(key)
        const player = new Player(this._checksum)
        const shared = this._logs.slice()
        let index = 0
        for (const log of shared) {
            await log.sequester.share()
        }
        const logs = shared.filter(log => ! log.shifted)
        try {
            if (index == logs.length) {
                return
            }
            do {
                const log = this._logs[index++]
                const blocks = this._blocks[log.id][keyified].slice()
                const filename = path.join(this.directory, String(log.id))
                const handle = await WriteAhead.Error.resolve(fs.open(filename, 'r'), 'OPEN_ERROR', { filename })
                try {
                    while (blocks.length != 0) {
                        const block = blocks.shift()
                        const buffer = Buffer.alloc(block.length)
                        const { bytesRead } = await WriteAhead.Error.resolve(handle.read(buffer, 0, buffer.length, block.position), 'READ_ERROR', { filename })
                        WriteAhead.Error.assert(bytesRead == buffer.length, 'BLOCK_SHORT_READ', { filename })
                        const entries = player.split(buffer)
                        WriteAhead.Error.assert(entries.length == 1, 'BLOCK_MISSING', { filename })
                        yield entries[0].parts[1]
                    }
                } finally {
                    await WriteAhead.Error.resolve(handle.close(), 'CLOSE_ERROR', { filename })
                }
            } while (index != logs.length)
        } finally {
            shared.map(log => log.sequester.unlock())
        }
    }

    async *head () {
        if (this._logs.length > 1) {
            const log = this._logs[0]
            await log.sequester.share()
            try {
                if (!log.shifted) {
                    const player = new Player(this._checksum)
                    const filename = path.join(this.directory, String(log.id))
                    const stream = fileSystem.createReadStream(filename)
                    const readable = new Staccato.Readable(stream)
                    for await (const buffer of readable) {
                        const entries = player.split(buffer)
                        for (const entry of entries) {
                            const keys = JSON.parse(String(entry.parts[0]))
                            yield { keys, body: entry.parts[1] }
                        }
                    }
                }
            } finally {
                log.sequester.unlock()
            }
        }
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
    async write (entries) {
        const { _recorder: recorder } = this
        const log = this._logs[this._logs.length - 1]
        const filename = path.join(this.directory, String(log.id))
        const handle = await fs.open(filename, 'a')
        try {
            const stat = await handle.stat()
            let position = stat.size
            const buffers = [], positions = {}
            for (const entry of entries) {
                const { keys, body } = entry
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
        const log = {
            id: this._logs[this._logs.length - 1].id + 1,
            shifted: false,
            sequester: new Sequester
        }
        const filename = path.join(this.directory, String(log.id))
        await fs.writeFile(filename, '', { flags: 'wx' })
        this._logs.push(log)
        this._blocks[log.id] = []
    }

    async shift () {
        if (this._logs.length == 0) {
            return false
        }
        await this._logs[0].sequester.exclude()
        const log = this._logs.shift()
        log.shifted = true
        log.sequester.unlock()
        const filename = path.join(this.directory, String(log.id))
        await fs.unlink(filename)
        return true
    }
}

module.exports = WriteAhead
