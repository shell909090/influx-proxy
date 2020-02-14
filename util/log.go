package util

import (
    "github.com/sirupsen/logrus"
    "io"
    "os"
)

var (
    Log             *logrus.Logger
    RebalanceLog    *logrus.Logger
    RecoveryLog     *logrus.Logger
    ResyncLog       *logrus.Logger
)

func init() {
    CheckPathAndCreate("log")
    Log = newLogger(os.Stdout)

    rebalanceLogFile, err := os.OpenFile("log/rebalance.log", os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0644)
    if err != nil {
        panic(err)
    }
    RebalanceLog = newLogger(rebalanceLogFile)

    recoveryLogFile, err := os.OpenFile("log/recovery.log", os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0644)
    if err != nil {
        panic(err)
    }
    RecoveryLog = newLogger(recoveryLogFile)

    resyncLogFile, err := os.OpenFile("log/resync.log", os.O_WRONLY | os.O_APPEND | os.O_CREATE, 0644)
    if err != nil {
        panic(err)
    }
    ResyncLog = newLogger(resyncLogFile)
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
