influxdb proxy

# 指令识别和分布

支持的指令：

* select: 可以select，但是不能into，否则有可能into到不正确的实例上。
* revoke: 支持。
* delete: 必须有from子句。
* drop series: 必须有from子句。
* show field keys: 必须有from子句。
* show series: 必须有from子句。
* show tag keys: 必须有from子句。
* show tag values: 必须有from子句。
* show databases: 支持。
