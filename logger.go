package main

import (
	"bytes"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"

	"github.com/sirupsen/logrus"
)

// @Author Chen Zikang
// @Email  parakovo@gmail.com
// @Since  2022-08-16

var (
	logger *logrus.Logger
)

func init() {
	logger = NewLogger(logrus.DebugLevel.String(), os.Stdout)
}

const (
	logDirPath        = "log/"
	logFileSuffix     = ".log"
	timeFormat        = "2006-01-02 15:04:05.999"
	maxFunctionLength = 30
)

func NewLogger(lvl string, output io.Writer) *logrus.Logger {
	level, err := logrus.ParseLevel(lvl)
	if err != nil {
		level = logrus.InfoLevel
	}
	l := logrus.New()
	l.Level = level
	l.SetReportCaller(true)
	l.SetFormatter(&LogFormatter{})
	l.SetOutput(output)
	return l
}

func LogFile(filename string) *os.File {
	_, err := os.Stat(logDirPath)
	if os.IsNotExist(err) {
		if err = os.MkdirAll(logDirPath, 0777); err != nil {
			log.Fatalf("Create log dir %s failed: %s", logDirPath, err)
		}
	}
	filename = filename + logFileSuffix
	filename = path.Join(logDirPath, filename)
	if _, err = os.Stat(filename); err != nil {
		if _, err = os.Create(filename); err != nil {
			log.Fatalf("Create log file %s failed:%s", filename, err)
		}
	}
	logFile, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY, os.ModeAppend)
	if err != nil {
		log.Fatalf("Open log file %s failed: %s", filename, err)
	}
	return logFile
}

type LogFormatter struct{}

func (f *LogFormatter) Format(entry *logrus.Entry) ([]byte, error) {
	var buf *bytes.Buffer
	if entry.Buffer != nil {
		buf = entry.Buffer
	} else {
		buf = &bytes.Buffer{}
	}

	datetime := entry.Time.Format(timeFormat)
	if len(datetime) < len(timeFormat) {
		for i := 0; i < len(timeFormat)-len(datetime); i++ {
			datetime = datetime + "0"
		}
	}
	logLevel := strings.ToUpper(entry.Level.String())
	if len(logLevel) < 5 {
		logLevel = logLevel + " "
	}
	if len(logLevel) > 5 {
		logLevel = logLevel[:5]
	}

	function := entry.Caller.Function[strings.LastIndex(entry.Caller.Function, "/")+1:]
	funcLen := len(function)
	if funcLen < maxFunctionLength {
		for i := 0; i < maxFunctionLength-funcLen; i++ {
			function = function + " "
		}
	} else if funcLen > maxFunctionLength {
		function = function[len(function)-maxFunctionLength:]
	}
	var fieldStr string
	for key, val := range entry.Data {
		fieldStr = fieldStr + fmt.Sprintf("%s=%v ", key, val)
	}

	// datetime level [function] - message filedStr
	logStr := fmt.Sprintf("%s %s [%s] - %s %s\n", datetime, logLevel, function, entry.Message, fieldStr)
	buf.WriteString(logStr)
	return buf.Bytes(), nil
}
