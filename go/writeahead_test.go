package writeahead

import (
    "testing"
    "os"
/*    "bytes"
    "encoding/gob"
    "fmt"
    "log" */
)

func NullChecksum (_ []byte) int {
    return 0
}

func TestWriteAhead (t *testing.T) {
    os.RemoveAll("tmp")
    _ = os.Mkdir("tmp", 0755)
    NewWriteAhead("tmp", NullChecksum)
}
