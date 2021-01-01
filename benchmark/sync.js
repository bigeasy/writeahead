const fs = require('fs').promises
const path = require('path')

const WRITES = process.argv[2] ? +process.argv[2] : 1
const SIZE = process.argv[3] ? +process.argv[3] : 1

const buffer = Buffer.alloc(SIZE)
const buffers = []

for (let i = 0; i < WRITES; i++) {
    buffers.push(buffer)
}

console.log(buffers.length)

async function main () {
    await fs.rmdir(path.join(__dirname, 'tmp'), { recursive: true })
    await fs.mkdir(path.join(__dirname, 'tmp'), { recursive: true })


    {
        const start = Date.now()
        const handle = await fs.open(path.join(__dirname, 'tmp', 'none'), 'a')
        for (let i = 0; i < WRITES; i++) {
            await handle.write(buffer)
        }
        await handle.close()
        console.log('none   ', Date.now() - start)
    }

    {
        const start = Date.now()
        for (let i = 0; i < WRITES; i++) {
            await fs.writeFile(path.join(__dirname, 'tmp', 'writeFile'), buffer, { flag: 'as' })
        }
        console.log('writeFile  ', Date.now() - start)
    }

    {
        const start = Date.now()
        const handle = await fs.open(path.join(__dirname, 'tmp', 'O_SYNC'), 'as')
        for (let i = 0; i < WRITES; i++) {
            await handle.write(buffer)
        }
        await handle.close()
        console.log('O_SYNC  ', Date.now() - start)
    }

    {
        const start = Date.now()
        const handle = await fs.open(path.join(__dirname, 'tmp', 'writev'), 'as')
        await handle.writev(buffers)
        await handle.close()
        console.log('O_SYNC  writev', Date.now() - start)
    }

    {
        const start = Date.now()
        const handle = await fs.open(path.join(__dirname, 'tmp', 'datasync'), 'a')
        for (let i = 0; i < WRITES; i++) {
            await handle.write(buffer)
        }
        await handle.datasync()
        await handle.close()
        console.log('datasync', Date.now() - start)
    }

    {
        const start = Date.now()
        const handle = await fs.open(path.join(__dirname, 'tmp', 'sync'), 'a')
        for (let i = 0; i < WRITES; i++) {
            await handle.write(buffer)
        }
        await handle.sync()
        await handle.close()
        console.log('sync    ', Date.now() - start)
    }
    {
        const start = Date.now()
        const handle = await fs.open(path.join(__dirname, 'tmp', 'swritev'), 'as')
        await handle.writev(buffers)
        await handle.sync()
        await handle.close()
        console.log('sync writev', Date.now() - start)
    }
}

main()
