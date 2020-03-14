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
    Version            = "2.3.0"
)

var (
    ForbidCmds  = []string{"(?i:^grant|^revoke|^select.+into.+from)"}
    SupportCmds = []string{"(?i:from|^drop\\s+measurement)"}
    ClusterCmds = []string{
        "(?i:^show\\s+measurements|^show\\s+series|^show\\s+databases$)",
        "(?i:^show\\s+field\\s+keys|^show\\s+tag\\s+keys|^show\\s+tag\\s+values)",
        "(?i:^show\\s+stats)",
        "(?i:^show\\s+retention\\s+policies)",
        "(?i:^create\\s+database\\s+\"*([^\\s\";]+))",
    }
)
