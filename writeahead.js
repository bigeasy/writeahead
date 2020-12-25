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
const Staccato = require('staccato/redux')

let latch

class WriteAhead {
    static Error = Interrupt.create('WriteAhead.Error', {
        'IO_ERROR': 'i/o error',
        'NO_LOGS': 'attempt to write when no logs exist',
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

    constructor ({ directory, logs, checksum, blocks, handle, position }) {
        this.directory = directory
        this._logs = logs
        this._recorder = Recorder.create(checksum)
        this._blocks = blocks
        this._checksum = checksum
        this._handle = handle
        this._position = position
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
            const filename = path.join(directory, String(id))
            const stream = fileSystem.createReadStream(filename)
            const staccato = new Staccato($ => $(), stream, { filename })
            let position = 0, remainder = 0
            try {
                for await (const block of staccato) {
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
            } finally {
                staccato.close()
            }
        }
        const logs = ids.map(id => ({ id, shifted: false, sequester: new Sequester }))
        // Order here is so that handle open comes last so I don't have to add a
        // catch block with a close.
        if (logs.length == 0) {
            blocks[0] = []
            const log = {
                id: 0,
                shifted: false,
                sequester: new Sequester
            }
            logs.push(log)
            const filename = path.join(directory, String(log.id))
            await WriteAhead.Error.resolve(fs.writeFile(filename, '', { flags: 'wx' }), 'IO_ERROR', { filename })
        }
        const log = logs[logs.length - 1]
        const filename = path.join(directory, String(log.id))
        const stat = await WriteAhead.Error.resolve(fs.stat(filename), 'IO_ERROR', { filename })
        const position = stat.size
        const handle = await WriteAhead.Error.resolve(fs.open(filename, 'a'), 'IO_ERROR', { filename })
        return new WriteAhead({ directory, logs, checksum, blocks, handle, position })
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
                    const staccato = new Staccato(stream)
                    try {
                        for await (const buffer of staccato) {
                            const entries = player.split(buffer)
                            for (const entry of entries) {
                                const keys = JSON.parse(String(entry.parts[0]))
                                yield { keys, body: entry.parts[1] }
                            }
                        }
                    } finally {
                        staccato.close()
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
        WriteAhead.Error.assert(this._handle != null, 'NO_LOGS')
        const { _recorder: recorder } = this
        const log = this._logs[this._logs.length - 1]
        const buffers = [], positions = {}
        for (const entry of entries) {
            const { keys, body } = entry
            const keyified = keys.map(key => Keyify.stringify(key))
            const block = recorder([[ Buffer.from(JSON.stringify(keys)) ], [ body ]])
            for (const key of keyified) {
                positions[key] || (positions[key] = [])
                positions[key].push({ position: this._position, length: block.length })
            }
            this._position += block.length
            buffers.push(block)
        }
        const filename = path.join(this.directory, String(log.id))
        await WriteAhead.Error.resolve(this._handle.appendFile(Buffer.concat(buffers)), 'IO_ERROR', { filename })
        for (const key in positions) {
            this._blocks[log.id][key] || (this._blocks[log.id][key] = [])
            this._blocks[log.id][key].push.apply(this._blocks[log.id][key], positions[key])
        }
    }

    async sync () {
        if (this._handle != null) {
            const log = this._logs[this._logs.length - 1]
            const filename = path.join(this.directory, String(log.id))
            await WriteAhead.Error.resolve(this._handle.sync(), 'IO_ERROR', { filename })
        }
    }

    async rotate () {
        if (this._handle != null) {
            const log = this._logs[this._logs.length - 1]
            const filename = path.join(this.directory, String(log.id))
            const handle = this._handle
            this._handle = null
            await WriteAhead.Error.resolve(handle.close(), 'IO_ERROR', { filename })
        }
        const log = {
            id: this._logs.length == 0 ? 0 : this._logs[this._logs.length - 1].id + 1,
            shifted: false,
            sequester: new Sequester
        }
        const filename = path.join(this.directory, String(log.id))
        this._position = 0
        this._handle = await WriteAhead.Error.resolve(fs.open(filename, 'ax'), 'IO_ERROR', { filename })
        this._logs.push(log)
        this._blocks[log.id] = []
    }

    async shift () {
        if (this._logs.length == 0) {
            return false
        }
        await this._logs[0].sequester.exclude()
        const log = this._logs.shift()
        const filename = path.join(this.directory, String(log.id))
        const handle = this._handle
        this._handle = null
        await WriteAhead.Error.resolve(handle.close(), 'IO_ERROR', { filename })
        log.shifted = true
        log.sequester.unlock()
        await WriteAhead.Error.resolve(fs.unlink(filename), 'IO_ERROR', { filename })
        return true
    }

    async close () {
        if (this._handle != null) {
            const log = this._logs[this._logs.length - 1]
            const filename = path.join(this.directory, String(log.id))
            const handle = this._handle
            this._handle = null
            await WriteAhead.Error.resolve(handle.close(), 'IO_ERROR', { filename })
        }
    }
}

module.exports = WriteAhead
