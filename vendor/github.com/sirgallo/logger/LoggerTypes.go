package logger


// CustomLog
//  The custom logger struct
type CustomLog struct {
  // Name: the name of the log
  Name string
}

type LogLevel = string
type LogColor = string 

const (
  // Debug log level
  Debug LogLevel = "Debug"
  // Error log level
  Error LogLevel = "Error"
  // Info log level
  Info LogLevel = "Info"
  // Warn log level
  Warn LogLevel = "Warn"
)

const Reset = "\033[0m"
const Bold = "\033[1m"

// ANSI escape codes for text colors
const (
  // DebugColor: blue
  DebugColor LogColor = "\033[34m"
  // ErrorColor: red
  ErrorColor LogColor = "\033[31m"
  // InfoColor: green
  InfoColor LogColor = "\033[32m"
  // WarnColor: yellow
  WarnColor LogColor = "\033[33m"
)

// TimeFormat 
//  Timestamp format in the log
const TimeFormat = "2006-01-02 15:04:05.000"