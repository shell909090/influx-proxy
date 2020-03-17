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

func SetMLog(dir, name string) {
    logPath := filepath.Join(dir, name)
    if logPath == "" {
        Mlog.SetOutput(os.Stdout)
    } else {
        MakeDir(dir)
        Mlog.SetOutput(&lumberjack.Logger{
            Filename:   logPath,
            MaxSize:    100,
            MaxBackups: 5,
            MaxAge:     7,
        })
    }
}
