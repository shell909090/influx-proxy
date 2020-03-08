package util

import (
    "math"
)

const (
    MaxUint32          = math.MaxUint32 + 1
    IdleTimeOut        = 10
    CheckPingInterval  = 1
    WaitActiveInterval = 10
    SyncFileInterval   = 10
    MigrateBatchSize   = 10000
    CipherKey          = "3kcdplq90m438j5h3n3es0lm"
    Version            = "2.2.1"
)

var (
    ForbidCmds  = []string{"(?i:^grant|^revoke|^select.+into.+from)"}
    SupportCmds = []string{"(?i:from|^drop\\s+measurement)"}
    ClusterCmds = []string{
        "(?i:^show\\s+measurements|^show\\s+series|^show\\s+databases$)",
        "(?i:^show\\s+field\\s+keys|^show\\s+tag\\s+keys)",
        "(?i:^show\\s+retention\\s+policies)",
    }
)
