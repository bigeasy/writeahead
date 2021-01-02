const fs = require('fs').promises

exports.read = async function* (resolve, filename) {
    const buffer = Buffer.alloc(1024 * 1024)
    const handle = await resolve(fs.open(filename, 'r'))
    try {
        for (;;) {
            const { bytesRead } = await resolve(handle.read(buffer, 0, buffer.length))
            if (bytesRead == 0) {
                break
            }
            yield bytesRead < buffer.length ? buffer.slice(0, bytesRead) : buffer
        }
    } finally {
        await resolve(handle.close())
    }
}
