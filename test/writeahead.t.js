// Throughout this document you are going to read the design decisions and the
// rationalizations behind them. At the time of this writing they are
// assumptions. Perhaps someday they will be convictions.

//
require('proof')(8, async okay => {
    const WriteAhead = require('..')
    const Destructible = require('destructible')
    const Turnstile = require('turnstile')
    //

    // We use Transcript to create our example records. Transcript is used
    // internally by WriteAhead but it is not required for API usage. Entries
    // are an array of JSON serializable keys and a `Buffer` of any format.

    //
    const { Recorder, Player } = require('transcript')
    //

    // We are going to serialize our records using a Transcript recorder with a
    // dummy checksum.

    //
    const recorder = Recorder.create(() => 0)
    //

    // TK DOCUMENT THIS

    //
    const Fracture = require('fracture')

    // We include the basics of the Node.js API.

    //
    const path = require('path')
    const fs = require('fs').promises
    //

    // We need a temporary directory for our unit test. We destroy it and
    // recreate it on each run.

    //
    const directory = path.join(__dirname, 'tmp', 'writeahead')

    const { coalesce } = require('extant')

    await coalesce(fs.rm, fs.rmdir).call(fs, directory, { force: true, recursive: true })
    await fs.mkdir(directory, { recursive: true })
    //

    // We've used Interrupt as the base for `WriteAhead.Error`. This auditing
    // that we run only during unit testing will ensure that even the assertions
    // we do not raise in this unit test will be formatted correctly when they
    // are raised in an application.

    //
    const Interrupt = require('interrupt')

    Interrupt.audit = function (error, errors) {
        if (error instanceof WriteAhead.Error && errors.length) {
            throw new Error
        }
    }
    //

    // Here we write out a set of records, then read them back immediately.

    //
    {
        // Create a write ahead object. The `directory` is where the write ahead
        // log will be written. The directory should be empty and dedicated to
        // the write ahead log.

        //
        const open = await WriteAhead.open({ directory })
        const destructible = new Destructible('writeahead.t')
        const turnstile = new Turnstile(destructible.durable({ isolated: true }, 'turnstile'))
        const writeahead = new WriteAhead(destructible.durable('writeahead'), turnstile, open)
        destructible.ephemeral($ => $(), 'writeahead', async () => {
            //

            // We write out a set of keys and a buffer. (Maybe if we want this
            // readme to be really easy to understand we should just serialize
            // plan text lines and simple keys.) Maybe our more complicated
            // example gets pushed to the end of the file.

            // The write will be an append to the write ahead file. There is no
            // synchronization for the write, so you can only have one write at
            // a time. You can have multiple concurrent readers, however.

            //
            const promise = writeahead.write(Fracture.stack(), [{
                keys: [ 0, 1 ],
                buffer: Buffer.from('a')
            }, {
                keys: [ 0 ],
                buffer: Buffer.from('b')
            }, {
                keys: [ 1 ],
                buffer: Buffer.from('c')
            }], true)
            //

            // You can get these written values immediately from memory before
            // they are even written to disk.

            //
            const gathered = []
            for await (const block of writeahead.get(1)) {
                gathered.push(block.toString())
            }

            okay(gathered, [ 'a', 'c' ], 'read from memory')
            //
            await promise
            //

            // To read we create an asynchronous iterator that returns blocks.

            // I decided to create a custom asynchronous iterator instead of
            // implementing readable stream. I don't need to stream the contents
            // of my write-head logs, I'm not piping them from the write-ahead
            // log to a file. I'm parsing each block at it is returned and using
            // the contents to update a b-tree or r-tree. The streaming is
            // unnecessary.

            // I did try implementing a readable stream, but my applications are
            // `async`/`await` and `async`/`await` behavior for `ReadStream`
            // changed between Node.js 12 and Node.js 14. If you are `async`
            // iterating over `ReadStream` in `destroy()` early in Node.js 12,
            // the error is emitted as an event, in Node.js 14 the error is
            // raised as an exception. I did not want to have a version
            // dependent unit test on a library that had not yet been released
            // so either I drop support for Node.js 12 or use a different
            // interface.

            // If you really want to stream from a write-ahead log, there are
            // utilities on NPM like
            // [async-iterator-to-stream](https://github.com/JsCommunity/async-iterator-to-stream).
            // Not endorsing this module, merely noting that it is a candidate
            // and I'm happy to have you antagonize those maintainers with
            // stream implementation issues instead of me.

            //
            gathered.length = 0
            for await (const block of writeahead.get(1)) {
                gathered.push(block.toString())
            }

            okay(gathered, [ 'a', 'c' ], 'read from file')

            destructible.destroy()
        })

        await destructible.promise
    }

    {
        const open = await WriteAhead.open({ directory })
        const destructible = new Destructible('writeahead.t')
        const turnstile = new Turnstile(destructible.durable({ isolated: true }, 'turnstile'))
        const writeahead = new WriteAhead(destructible.durable('writeahead'), turnstile, open)

        destructible.ephemeral($ => $(), 'writeahead', async () => {
            const gathered = []
            for await (const block of writeahead.get(1)) {
                gathered.push(block.toString())
            }

            okay(gathered, [ 'a', 'c' ], 'reopened')

            destructible.destroy()
        })

        await destructible.promise
    }

    {
        const open = await WriteAhead.open({ directory })
        const destructible = new Destructible('writeahead.t')
        const turnstile = new Turnstile(destructible.durable({ isolated: true }, 'turnstile'))
        const writeahead = new WriteAhead(destructible.durable('writeahead'), turnstile, open)

        destructible.ephemeral($ => $(), 'writeahead', async () => {
            await writeahead.rotate(Fracture.stack())

            await writeahead.write(Fracture.stack(), [{
                keys: [ 0 ],
                buffer: Buffer.from('d')
            }, {
                keys: [ 0, 1 ],
                buffer: Buffer.from('e')
            }], true)

            const gathered = []
            for await (const block of writeahead.get(1)) {
                gathered.push(block.toString())
            }

            okay(gathered, [ 'a', 'c', 'e' ], 'rotated')

            destructible.destroy()
        })

        await destructible.promise
    }

    {
        const open = await WriteAhead.open({ directory })
        const destructible = new Destructible('writeahead.t')
        const turnstile = new Turnstile(destructible.durable({ isolated: true }, 'turnstile'))
        const writeahead = new WriteAhead(destructible.durable('writeahead'), turnstile, open)

        destructible.ephemeral($ => $(), 'writeahead', async () => {
            const gathered = []
            for await (const block of writeahead.get(1)) {
                gathered.push(block.toString())
            }

            okay(gathered, [ 'a', 'c', 'e' ], 'rotated reopened')

            await writeahead.shift(Fracture.stack())

            {
                gathered.length = 0
                for await (const block of writeahead.get(1)) {
                    gathered.push(block.toString())
                }
            }

            okay(gathered, [ 'e' ], 'shifted')
            destructible.destroy()
        })

        await destructible.promise
    }

    {
        const open = await WriteAhead.open({ directory })
        const destructible = new Destructible('writeahead.t')
        const turnstile = new Turnstile(destructible.durable({ isolated: true }, 'turnstile'))
        const writeahead = new WriteAhead(destructible.durable('writeahead'), turnstile, open)

        destructible.ephemeral($ => $(), 'writeahead', async () => {
            const gathered = []
            for await (const block of writeahead.get(1)) {
                gathered.push(block.toString())
            }

            okay(gathered, [ 'e' ], 'shifted reopened')

            await writeahead.shift(Fracture.stack())

            {
                gathered.length = 0
                for await (const block of writeahead.get(0)) {
                    gathered.push(block.toString())
                }
            }

            okay(gathered, [], 'shifted to empty')

            await writeahead.shift(Fracture.stack())

            // Rotate when there is no log.
            await writeahead.rotate(Fracture.stack())

            destructible.destroy()
        })

        await destructible.promise
    }
    return
    {
        const writeahead = await WriteAhead.open({ directory })

        await writeahead.write([{
            keys: [ 0, 1 ],
            buffer: Buffer.from('a')
        }, {
            keys: [ 0 ],
            buffer: Buffer.from('b')
        }, {
            keys: [ 1 ],
            buffer: Buffer.from('c')
        }])

        await fs.unlink(path.join(__dirname, 'tmp', 'writeahead', '0'))

        const errors = []

        try {
            const gathered = []
            for await (const block of writeahead.get(0)) {
                gathered.push(block.toString())
            }
        } catch (error) {
            console.log(error.stack)
            errors.push(error.code)
        }

        okay(errors, [ 'IO_ERROR' ], 'open error')

        await writeahead.close()
    }
})
