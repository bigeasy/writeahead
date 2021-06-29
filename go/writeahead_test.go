package writeahead

import (
    "testing"
/*    "bytes"
    "encoding/gob"
    "fmt"
    "log" */
)

func NullChecksum (_ []byte) int {
    return 0
}

func TestWriteAhead (t *testing.T) {
    NewWriteAhead("tmp", NullChecksum)
}
