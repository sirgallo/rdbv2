# Array

## A collection of array utilities implemented in Go


## Usage

```go
package main

import "github.com/sirgallo/array"


func main() {
  dummyArr := []int{ 1, 2, 3, 4 }

  // chunk an array
  chunked, chunkErr := array.Chunk[int](dummyArr, 2)
  if chunkErr != nil { panic(chunkErr.Error()) }

  // filter an array
  filterFunc := func(elem int) bool { return elem < 3 }
  filtered := array.Filter[int](dummyArr, filterFunc)

  // map an array
  mapFunc := func(elem int) int { return elem * 2 }
  mapped := array.Map[int](dummyArr, mapFunc)
}
```

## Tests

`array`
```bash
go test -v ./tests
```

## godoc

For in depth definitions of types and functions, `godoc` can generate documentation from the formatted function comments. If `godoc` is not installed, it can be installed with the following:
```bash
go install golang.org/x/tools/cmd/godoc
```

To run the `godoc` server and view definitions for the package:
```bash
godoc -http=:6060
```

Then, in your browser, navigate to:
```
http://localhost:6060/pkg/github.com/sirgallo/array/
```