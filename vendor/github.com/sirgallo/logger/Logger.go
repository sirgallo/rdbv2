package logger

import "fmt"
import "os"
import "strings"
import "time"

import "github.com/sirgallo/array"
import "github.com/sirgallo/utils"


//============================================= Logger


// NewCustomLog
//	Initilize a new custom logger.
//
// Parameters:
//	name: the name of the log
//
// Returns:
//	Initialized custom logger
func NewCustomLog(name string) *CustomLog {
	return &CustomLog{
		Name: name,
	}
}

// Debug, Error, Info, Warn:
//	Different log levels.
func (cLog *CustomLog) Debug(msg ...interface{}) {
	cLog.formatOutput(Debug, msg)
} 

func (cLog *CustomLog) Error(msg ...interface{}) {
	cLog.formatOutput(Error, msg)
} 

func (cLog *CustomLog) Info(msg ...interface{}) {
	cLog.formatOutput(Info, msg)
} 

func (cLog *CustomLog) Warn(msg ...interface{}) {
	cLog.formatOutput(Warn, msg)
}

func (cLog *CustomLog) Fatal(msg ...interface{}) {
	cLog.formatOutput(Error, msg)
	os.Exit(1)
}

// formatOutput
//	helper method for each of the log levels output is:
//	(formatted time) [name] Log level: encoded message
//
// Parameters:
//	level: the log level (debug, error, info, warn, fatal)
//	msg: the message to be formatted
func (cLog *CustomLog) formatOutput(level LogLevel, msg []interface{}) {
	currTime := time.Now()
	formattedTime := currTime.Format(TimeFormat)

	encodedMsg := func() string {
		encodeTransform := func(chunk interface{}) string {
			encoded, _ := utils.EncodeStructToJSONString[interface{}](chunk)
			return encoded
		}
	
		encodedChunks := array.Map[interface{}, string](msg, encodeTransform)
		return strings.Join(encodedChunks, " ")
	}()

	color := func() LogColor {
		if level == Debug { 
			return DebugColor 
		} else if level == Error { 
			return ErrorColor
		} else if level == Info {
			return InfoColor
		} else { return WarnColor }
	}()

	fmt.Printf("%s(%s) [%s] %s: %s\n", color, formattedTime, cLog.Name, Bold + level, Reset + encodedMsg)
}