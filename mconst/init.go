package mconst

import (
    "github.com/pkg/errors"
    "math"
)

const (
    MaxUint32          = math.MaxUint32 + 1
    IdleTimeOut        = 10
    CheckPingTimeOut   = 10
    AwaitActiveTimeOut = 10
    SyncFileData       = 5
    Version            = "3.0.1"
)

var (
    Success        = 200
    NotComplete    = 202
    SuccessNoResp  = 204
    BadRequest     = 400
    MethodNotAllow = 405
)

var Code2Message = map[int][]byte{
    Success:        []byte("请求成功"),
    NotComplete:    []byte("请求已在处理中"),
    BadRequest:     []byte("客户端请求存在错误"),
    MethodNotAllow: []byte("客户端请求方法被禁止"),
}

var (
    SyncAllProxy = []byte("请告知所有proxy，转移读操作流量")
)

// http method
const (
    Post = "POST"
    Get  = "GET"
)

var (
    FormatNilErr = errors.New("format error")
    LengthNilErr = errors.New("length is zero")
)

var (
    ForbidCmds   = "(?i:^\\s*grant|^\\s*revoke|\\(\\)\\$)"
    SupportCmds  = "(?i:from|drop\\s*measurement)"
    ExecutorCmds = "(?i:show\\s*measurements|show\\s*tag\\s*keys|show\\s*series|show\\s*field\\s*keys)"
)
