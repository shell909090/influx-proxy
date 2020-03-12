package util

import (
    "gopkg.in/natefinch/lumberjack.v2"
    "log"
    "os"
    "path/filepath"
)

var Mlog *log.Logger

func init() {
    log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
    log.SetOutput(os.Stdout)
    Mlog = log.New(os.Stdout, "", log.LstdFlags | log.Lmicroseconds | log.Lshortfile)
}

func SetMLog(logPath string, prefix string) {
    Mlog.SetPrefix(prefix)
    CheckPathAndCreate(filepath.Dir(logPath))
    if logPath == "" {
        Mlog.SetOutput(os.Stdout)
    } else {
        Mlog.SetOutput(&lumberjack.Logger{
            Filename:   logPath,
            MaxSize:    100,
            MaxBackups: 5,
            MaxAge:     7,
        })
    }
}
