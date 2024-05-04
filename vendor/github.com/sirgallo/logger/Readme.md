# Logger

## A custom logger written in Go


## Usage

```go
package main

import "github.com/sirgallo/logger"


func main() {
  // init the logger
  cLog := logger.NewCustomLog("Custom Log")

  // log for each log level
  cLog.Debug("this is a debug message.")
  cLog.Error("this is an error message.")
  cLog.Info("this is an info message.")
  cLog.Warn("this is a warning message.")
  cLog.Fatal("this is an error message that will terminate your program.")
}
```


## Tests

`logger`
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
http://localhost:6060/pkg/github.com/sirgallo/logger/
```