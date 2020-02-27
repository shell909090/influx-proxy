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
    CipherKey          = "3kcdplq90m438j5h3n3es0lm"
    Version            = "2.2.0"
)

var (
    ForbidCmds   = "(?i:^\\s*grant|^\\s*revoke|\\(\\)\\$)"
    SupportCmds  = "(?i:from|drop\\s+measurement)"
    ClusterCmds  = "(?i:show\\s+databases|show\\s+series|show\\s+measurements|show\\s+tag\\s+keys|show\\s+field\\s+keys)"
)
