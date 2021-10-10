[![Actions Status](https://github.com/bigeasy/writeahead/workflows/Node%20CI/badge.svg)](https://github.com/bigeasy/writeahead/actions)
[![codecov](https://codecov.io/gh/bigeasy/writeahead/branch/master/graph/badge.svg)](https://codecov.io/gh/bigeasy/writeahead)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A write-ahead log.

| What          | Where                                             |
| --- | --- |
| Discussion    | https://github.com/bigeasy/writeahead/issues/1    |
| Documentation | https://bigeasy.github.io/writeahead              |
| Source        | https://github.com/bigeasy/writeahead             |
| Issues        | https://github.com/bigeasy/writeahead/issues      |
| CI            | https://travis-ci.org/bigeasy/writeahead          |
| Coverage:     | https://codecov.io/gh/bigeasy/writeahead          |
| License:      | MIT                                               |


WriteAhead installs from NPM.

```
//{ "mode": "text" }
npm install writeahead
```

## Living `README.md`

This `README.md` is also a unit test using the
[Proof](https://github.com/bigeasy/proof) unit test framework. We'll use the
Proof `okay` function to assert out statements in the readme. A Proof unit test
generally looks like this.

```javascript
//{ "code": { "tests": 6 }, "text": { "tests": 4  } }
require('proof')(%(tests)d, async okay => {
    //{ "include": "test", "mode": "code" }
    //{ "include": "proof" }
})
```

```javascript
//{ "name": "proof", "mode": "text" }
okay('always okay')
okay(true, 'okay if true')
okay(1, 1, 'okay if equal')
okay({ value: 1 }, { value: 1 }, 'okay if deep strict equal')
```

You can run this unit test yourself to see the output from the various
code sections of the readme.

```text
//{ "mode": "text" }
git clone git@github.com:bigeasy/writeahead.git
cd writeahead
npm install --no-package-lock --no-save
node test/readme.t.js
```

## Overview


```javascript
//{ "name": "test", "mode": "code" }
// This will not appear in `README.md`.

// As noted, this `README.md` is also a unit test. We need a temporary directory
// to store our write-ahead log for the unit test. We delete it and recreate it
// on every run of the test.

// Node.js file system and file path APIs.
const path = require('path')
const fs = require('fs').promises

// Our directory will live under our test directory.
const directory = path.join(__dirname, 'tmp', 'writeahead')

// Remove the existing directory recursively with a hack to accommodate Node.js
// file system API deprecations.
await (fs.rm || fs.rmdir).call(fs, directory, { force: true, recursive: true })

// Create the temporary directory.
await fs.mkdir(directory, { recursive: true })
```

WriteAhead exports a single `WriteAhead` class.

```javascript
//{ "name": "test", "code": { "path": "'..'" }, "text": { "path": "'writeahead'" } }
const WriteAhead = require(%(path)s)
```

WriteAhead is designed to be used in concert with the structure concurrency
libraries [Turnstile](https://github.com/bigeasy/turnstile),
[Fracture](https://github.com/bigeasy/fracture), and
[Destructible](https://github.com/bigeasy/destructible).

If you're not interested in getting into the details of any of that, I don't
blame you, don't worry. Use of WriteAhead only requires a superficial use of
these libraries. You need to require them into your module but then you'll use
them according to a boilerplate.

```javascript
//{ "name": "test" }
const Turnstile = require('turnstile')
const Fracture = require('fracture')
const Destructible = require('destructible')
```

```javascript
//{ "name": "test", "mode": "code" }
{
    //{ "include": "first" }
}
```

A `WriteAhead` instance requires and instance of `Destructible` and `Turnstile`.
Here we create a destructible and give it a meaningful name. We then create a
`Trunstile` using a sub-destructible as its only argument. You can copy and
paste this boilerplate into your module.

The `WriteAhead` construction is itself a two step process of creating an
`opening` instance that is used to construct the `WriteAhead` object.

```javascript
//{ "name": "first" }
const path = require('path')

const destructible = new Destructible('writeahead.t')
const turnstile = new Turnstile(destructible.durable('turnstile'))

const open = await WriteAhead.open({ directory: path.join(__dirname, 'tmp', 'writeahead') })
const writeahead = new WriteAhead(destructible.durable('writeahead'), turnstile, open)

//{ "include": "first.strand" }
```

```javascript
//{ "name": "first.strand", "mode": "code" }
destructible.ephemeral($ => $(), 'writeahead', async () => {
    //{ "include": "first.body" }
})

await destructible.promise
```

The first argument to 'writeahead.write` is a Fracture stack which you can
create using `Fracture.stack()`.

The second argument is an array of objects that contain a buffer to write to the
write-ahead log and a set of one or more JSON serializable keys that you can use
to select the records out of the write-ahead log when reading.

The final argument is the `sync` flag which determines if the entries should be
written to disk with with an `fsync`. The `fsync` will ensure that all the
entries are synced to the underlying storage medium when the `write` function
returns.

```javascript
//{ "name": "first.body" }
const entries = [{
    keys: [ 0, 1 ],
    buffer: Buffer.from('a')
}, {
    keys: [ 0 ],
    buffer: Buffer.from('b')
}, {
    keys: [ 1 ],
    buffer: Buffer.from('c')
}]

await writeahead.write(Fracture.stack(), entries, true)
```

If you give a `sync` value of `false` the entries are written without
subsequently calling `fsync`. You may want to do this if you want to write a
series of entries that are part of a transaction and then write a final entry
that commits that transaction. `fsync` is somewhat expensive of if you can forgo
unnecssary calls to `fsync` your performance may improve.

To read we create an asynchronous iterator that returns the buffers for the
entries that match a given key in the order in which they were written to the
write-ahead log.

```javascript
//{ "name": "first.body" }
const gathered = []
for await (const block of writeahead.get(1)) {
    gathered.push(block.toString())
}

okay(gathered, [ 'a', 'c' ], 'read from file')
```

TODO Show the `0` key.

I decided to use an asynchronous iterator instead of implementing a `Readable`
stream. I don't need to stream the contents of my write-ahead logs, I'm not
pipeing them to a file or a socket. I'm parsing each block as it is returned and
using the contents to update a b-tree or r-tree. The streaming interface is a
lot of overheadto use an asynchronous iterator instead of implementing a
`Readable` stream. I don't need to stream the contents of my write-ahead logs,
I'm not pipeing them to a file or a socket. I'm parsing each block as it is
returned and using the contents to update a b-tree or r-tree. The streaming
interface is a lot of overhead and the async iterator provides a surrogate for
back-pressure by only pulling a block at a time from the write-ahead log.

If you really want to stream from a write-ahead log, there are utilities on NPM
like
[async-iterator-to-stream](https://github.com/JsCommunity/async-iterator-to-stream)
that convert an async iterator to a stream. I do not have experience with this
module, merely noting that it is a candidate.

When you are finished with the `WriteAhead` instance call the `destroy` method
of the `Destructible` used to create the `Turnstile` and `WriteAhead`. It will
wait for any queue writes to flush and close the write-ahead log.

```javascript
//{ "name": "first.body" }
destructible.destroy()
```

## Reopening

Reopening a write-ahead log is the same as creating it for the first time. Here
we reopen the write-ahead log we created above.

```javascript
//{ "unblock": true, "name": "test" }
{
    const path = require('path')

    const destructible = new Destructible('writeahead.t')
    const turnstile = new Turnstile(destructible.durable('turnstile'))

    const open = await WriteAhead.open({ directory: path.join(__dirname, 'tmp', 'writeahead') })
    const writeahead = new WriteAhead(destructible.durable('writeahead'), turnstile, open)

    //{ "include": "reopen.test" }
}
```

```javascript
//{ "name": "reopen.test", "mode": "code" }
destructible.ephemeral($ => $(), 'writeahead', async () => {
    //{ "include": "reopen.body" }
})

await destructible.promise
```

The records we wrote above are still there.

```javascript
//{ "name": "reopen.body" }
const gathered = []
for await (const block of writeahead.get(1)) {
    gathered.push(block.toString())
}

okay(gathered, [ 'a', 'c' ], 'read from reopened file')
```

Don't forge to close the write-ahead log when you're done with it.

```javascript
//{ "name": "reopen.body" }
destructible.destroy()
```

## Concurrency

The `write` method of `WriteAhead` is synchronous, even though it is an `async`
function, the initial write is to an in memory queue and the writes are
immediately available when the `write` function returns its `Promsie`.

This means that you can use a single write-ahead log in your program. All the
writes will be ordered in the order in which the `write` method was called.
There will be no interleaved writes.

Let's reopen our write-ahead log again.


```javascript
//{ "unblock": true, "name": "test" }
{
    const path = require('path')

    const destructible = new Destructible('writeahead.t')
    const turnstile = new Turnstile(destructible.durable('turnstile'))

    const open = await WriteAhead.open({ directory: path.join(__dirname, 'tmp', 'writeahead') })
    const writeahead = new WriteAhead(destructible.durable('writeahead'), turnstile, open)

    //{ "include": "concurrency.test" }
}
```

```javascript
//{ "name": "concurrency.test", "mode": "code" }
destructible.ephemeral($ => $(), 'writeahead', async () => {
    //{ "include": "concurrency.body" }
})

await destructible.promise
```

Now we write to the write ahead log but we do not await the returned `Promise`.
We hold onto it for a bit.

```javascript
//{ "name": "concurrency.body" }
const entries = [{
    keys: [ 0 ],
    buffer: Buffer.from('d')
}, {
    keys: [ 0, 1 ],
    buffer: Buffer.from('e')
}]

const promise = writeahead.write(Fracture.stack(), entries, true)
```

We can see that even though the asynchronous write has not completed we are
still able to read the written buffers from the in memory write queue. We know
that the writes are not written to disk. They can't be. We have to perform an
asynchronous write to the file and that cannot be performed until we `await` the
Promise.

```javascript
//{ "name": "concurrency.body" }
const gathered = []
for await (const block of writeahead.get(1)) {
    gathered.push(block.toString())
}

okay(gathered, [ 'a', 'c', 'e' ], 'read from in memory queue')
```

Of course, the written buffers will also be there after we `await` the `Promise`
returned from `write`.

```javascript
//{ "name": "concurrency.body" }
await promise

gathered.length = 0
for await (const block of writeahead.get(1)) {
    gathered.push(block.toString())
}

okay(gathered, [ 'a', 'c', 'e' ], 'read after write to file')
```

TODO Maybe don't open and close so much? It may make the documentation easier to
follow.

```javascript
//{ "name": "concurrency.body" }
destructible.destroy()
```

## Rotating

The write-ahead log cannot grow indefinately. We need to trim it eventually. To
do so we rotate the log, creating a new log file where new writes will be
written.

Let's reopen our write-ahead log again.

```javascript
//{ "unblock": true, "name": "test" }
{
    const path = require('path')

    const destructible = new Destructible('writeahead.t')
    const turnstile = new Turnstile(destructible.durable('turnstile'))

    const open = await WriteAhead.open({ directory: path.join(__dirname, 'tmp', 'writeahead') })
    const writeahead = new WriteAhead(destructible.durable('writeahead'), turnstile, open)

    //{ "include": "rotate.test" }
}
```

```javascript
//{ "name": "rotate.test", "mode": "code" }
destructible.ephemeral($ => $(), 'writeahead', async () => {
    //{ "include": "rotate.body" }
})

await destructible.promise
```

Rotate.

```javascript
//{ "name": "rotate.body" }
await writeahead.rotate(Fracture.stack())
```

Write to rotate.

```javascript
//{ "name": "rotate.body" }
const writes = [{
    keys: [ 1 ],
    buffer: Buffer.from('f')
}, {
    keys: [ 0 ],
    buffer: Buffer.from('g')
}]

await writeahead.write(Fracture.stack(), writes, true)
```

Everything is still there.

```javascript
//{ "name": "rotate.body" }
const gathered = []
for await (const block of writeahead.get(1)) {
    gathered.push(block.toString())
}

okay(gathered, [ 'a', 'c', 'e', 'f' ], 'added after rotate')
```

Now we can shift.

```javascript
//{ "name": "rotate.body" }
await writeahead.shift(Fracture.stack())
```

Everything before the rotate is gone.

```javascript
//{ "name": "rotate.body" }
gathered.length = 0
for await (const block of writeahead.get(1)) {
    gathered.push(block.toString())
}

okay(gathered, [ 'f' ], 'vacuumed after shift')
```

Let's close our write-ahead log.

```javascript
//{ "name": "rotate.body" }
destructible.destroy()
```

TODO Go look at Addendum and come back and write about how you use this IRL.
