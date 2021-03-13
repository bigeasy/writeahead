'use strict'

const assert = require('assert')
const path = require('path')
const fs = require('fs').promises

const { Recorder, Player } = require('transcript')

const Destructible = require('destructible')
const Operation = require('operation')
const Interrupt = require('interrupt')
const Keyify = require('keyify')
const Fracture = require('fracture')
const Future = require('perhaps')
const Sequester = require('sequester')

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
    //

    // Unlike other modules, WriteAhead manages its own Turnstile. It only ever
    // needs one background strand so we can just give it a single strand
    // Turnstile and not have to worry about the deadlock issues of a shared
    // Turnstile.

    //
    constructor (destructible, turnstile, { directory, logs, checksum, blocks, open, position, sync }) {
        this.destructible = destructible
        this.deferrable = destructible.durable($ => $(), { countdown: 1 }, 'deferrable')
        // Create a Fracture using a private Turnstile.
        this.turnstile = turnstile
        this._fracture = new Fracture(this.destructible.durable($ => $(), 'appender'), {
            turnstile: this.turnstile,
            value: name => {
                switch (name) {
                case 'write':
                    return { blocks: [], sync: false }
                case 'rotate':
                case 'shift':
                    return {}
                }
            },
            worker: this._background.bind(this)
        })
        this._fracture.deferrable.increment()
        this.destructible.destruct(() => this.deferrable.decrement())
        this.deferrable.destruct(() => {
            this.deferrable.ephemeral($ => $(), 'shutdown', async () => {
                await this.destructible.copacetic('drain', null, async () => this._fracture.drain())
                this._fracture.deferrable.decrement()
                if (this._open != null) {
                    const open = this._open
                    this._open = null
                    await this.sync.sync(open)
                    await WriteAhead.Error.resolve(open.handle.close(), 'IO_ERROR', open.properties)
                }
            })
        })
        this.directory = directory
        this._logs = logs
        this._recorder = Recorder.create(checksum)
        this._blocks = blocks
        this._checksum = checksum
        this._open = open
        this._position = position
        this.sync = sync
    }

    get position () {
        return this._position
    }

    static async open ({ directory, checksum = () => 0, sync = new Operation.Sync }) {
        const dir = await fs.readdir(directory)
        const ids = dir.filter(file => /^\d+$/.test(file))
                       .map(log => +log)
                       .sort((left, right) => left - right)
        const player = new Player(checksum)
        const blocks = {}
        for (const id of ids) {
            blocks[id] = {}
            const filename = path.join(directory, String(id))
            let position = 0, remainder = 0
            await Operation.read(filename, Buffer.alloc(1024 * 1024), ({ buffer }) => {
                let offset = 0
                for (;;) {
                    const [ entry ] = player.split(buffer.slice(offset), 1)
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
                remainder = buffer.length - offset
            })
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
        const open = { handle, properties: { filename } }
        return { directory, logs, checksum, blocks, open, position, sync }
    }
    //

    // Returns an asynchronous iterator over the blocks for the given key.

    //
    async *get (key) {
        const keyified = Keyify.stringify(key)
        const player = new Player(this._checksum)
        const shared = this._logs.slice()
        for (const log of shared) {
            await log.sequester.share()
        }
        const logs = shared.filter(log => ! log.shifted)
        try {
            for (let i = 0; i < logs.length; i++) {
                const log = this._logs[i]
                const filename = path.join(this.directory, String(log.id))
                const handle = await WriteAhead.Error.resolve(fs.open(filename, 'r'), 'OPEN_ERROR', { filename })
                const got = this._blocks[log.id][keyified] || []
                try {
                    for (let j = 0, J = got.length; j < J; j++) {
                        const block = got[j]
                        if (block.buffer != null) {
                            yield block.buffer
                        } else {
                            const buffer = Buffer.alloc(block.length)
                            const { bytesRead } = await WriteAhead.Error.resolve(handle.read(buffer, 0, buffer.length, block.position), 'READ_ERROR', { filename })
                            WriteAhead.Error.assert(bytesRead == buffer.length, 'BLOCK_SHORT_READ', { filename })
                            const entries = player.split(buffer)
                            WriteAhead.Error.assert(entries.length == 1, 'BLOCK_MISSING', { filename })
                            yield entries[0].parts[1]
                        }
                    }
                } finally {
                    await WriteAhead.Error.resolve(handle.close(), 'CLOSE_ERROR', { filename })
                }
            }
        } finally {
            shared.map(log => log.sequester.unlock())
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
    write (entries, sync = false) {
        this.deferrable.operational()
        const log = this._logs[this._logs.length - 1], blocks = []
        for (const entry of entries) {
            const { keys, buffer } = entry
            const block = { position: 0, length: 0, buffer }
            for (const key of keys.map(key => Keyify.stringify(key)) ) {
                this._blocks[log.id][key] || (this._blocks[log.id][key] = [])
                this._blocks[log.id][key].push(block)
            }
            blocks.push({ keys, block })
        }
        const enqueue = this._fracture.enqueue('write')
        enqueue.value.blocks.push.apply(enqueue.value.blocks, blocks)
        enqueue.value.sync = sync
        return enqueue.future
    }

    async _write (blocks) {
        if (this.deferrable.errored) {
            throw new Error
        }
        WriteAhead.Error.assert(this._open != null, 'NO_LOGS')
        const records = []
        for (const { keys, block } of blocks) {
            const record = (this._recorder)([[ Buffer.from(JSON.stringify(keys)) ], [ block.buffer ]])
            records.push(record)
            block.position = this._position
            block.length = record.length
            this._position += record.length
        }
        if (records.length != 0) {
            await Operation.writev(this._open, records)
        }
        for (const { block } of blocks) {
            block.buffer = null
        }
        this.writing = false
    }

    async _background ({ canceled, key, value, pause }) {
        await this.deferrable.copacetic($ => $(), 'write', null, async () => {
            switch (key) {
            case 'write': {
                    await this._write(value.blocks)
                    if (value.sync) {
                        await this.sync.sync(this._open)
                    }
                }
                break
            case 'rotate': {
                    const write = await pause('write')
                    try {
                        // Before we do anything asynchronous, we need to ensure that new
                        // writes are queued where the new log is supposed to be.
                        const log = {
                            id: this._logs.length == 0 ? 0 : this._logs[this._logs.length - 1].id + 1,
                            shifted: false,
                            sequester: new Sequester
                        }
                        this._logs.push(log)
                        this._blocks[log.id] = {}
                        const gathered = []
                        for (const entry of write.entries) {
                            gathered.push(entry.blocks)
                            entry.blocks = []
                        }
                        for (const blocks of gathered) {
                            await this._write(blocks)
                        }
                        this._position = 0
                        if (this._logs.length != 1) {
                            const open = this._open
                            this._open = null
                            await this.sync.sync(open)
                            await Operation.close(open)
                        }
                        this._open = await Operation.open(path.join(this.directory, String(log.id)), this.sync.flag)
                    } finally {
                        write.resume()
                    }
                }
                break
            case 'shift': {
                    if (this._logs.length != 0) {
                        await this._logs[0].sequester.exclude()
                        const log = this._logs.shift()
                        log.shifted = true
                        log.sequester.unlock()
                        if (this._logs.length == 0) {
                            const open = this._open
                            this._open = null
                            await this.sync.sync(open)
                            await Operation.close(open)
                        }
                        const filename = path.join(this.directory, String(log.id))
                        await WriteAhead.Error.resolve(fs.unlink(filename), 'IO_ERROR', { filename })
                    }
                }
                break
            }
        })
    }

    rotate () {
        return this._fracture.enqueue('rotate').future
    }

    shift () {
        return this._fracture.enqueue('shift').future
    }
}

module.exports = WriteAhead
