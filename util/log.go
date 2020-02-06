package util

import (
    "github.com/sirupsen/logrus"
    "io"
    "os"
)

var Log *logrus.Logger

func init() {
    CheckPathAndCreate("log")
    Log = newLogger(os.Stdout)
}

func newLogger(outPut io.Writer) *logrus.Logger {
    log := logrus.New()
    log.SetOutput(outPut)
    log.SetLevel(logrus.InfoLevel)
    log.SetFormatter(&logrus.TextFormatter{
        DisableColors: true,
    })
    log.ReportCaller = true
    return log
}
