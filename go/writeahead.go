package writeahead

type Block struct {
}

type WriteAhead struct {
    directory string
}

func NewWriteahead (directory string) *WriteAhead {
    return &WriteAhead{directory: directory}
}

func (writeahead *WriteAhead) Write () {
}
