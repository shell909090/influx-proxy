package util

import (
    "math"
)

const (
    MaxUint32          = math.MaxUint32 + 1
    IdleTimeOut        = 10
    CheckPingTimeOut   = 10
    AwaitActiveTimeOut = 10
    SyncFileData       = 5
    CIPHER_KEY         = "3kcdplq90m438j5h3n3es0lm"
    Version            = "2.1.1"
)

var (
    ForbidCmds   = "(?i:^\\s*grant|^\\s*revoke|\\(\\)\\$)"
    SupportCmds  = "(?i:from|drop\\s+measurement)"
    ClusterCmds  = "(?i:show\\s+databases|show\\s+series|show\\s+measurements|show\\s+tag\\s+keys|show\\s+field\\s+keys)"
)
