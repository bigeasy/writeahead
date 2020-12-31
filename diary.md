## Thu Dec 31 11:57:30 CST 2020

Since we're going to wait for `sync` to trust the contents anyway, we could have
write be synchronous and return an optional future, a very optional future,
where if someone is listening we actually sync, if not we mark it as resolved
and move on. Our block array references an object with a position or an array of
buffers.
