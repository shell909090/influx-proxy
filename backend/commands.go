// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

var (
    ForbidCmds   = "(?i:^\\s*grant|^\\s*revoke|\\(\\)\\$)"
    SupportCmds  = "(?i:from|drop\\s*measurement)"
    ExecutorCmds = "(?i:show.*measurements)"
)
