package backend

import (
	"strings"
	"testing"
)

// ALTER RETENTION POLICY "1h.cpu" ON "mydb" DEFAULT
// ALTER RETENTION POLICY "policy1" ON "somedb" DURATION 1h REPLICATION 4
// CREATE DATABASE "foo"
// CREATE DATABASE "bar" WITH DURATION 1d REPLICATION 1 SHARD DURATION 30m NAME "myrp"
// CREATE DATABASE "mydb" WITH NAME "myrp"
// CREATE RETENTION POLICY "10m.events" ON "somedb" DURATION 60m REPLICATION 2
// CREATE RETENTION POLICY "10m.events" ON "somedb" DURATION 60m REPLICATION 2 DEFAULT
// CREATE RETENTION POLICY "10m.events" ON "somedb" DURATION 60m REPLICATION 2 SHARD DURATION 30m
// CREATE SUBSCRIPTION "sub0" ON "mydb"."autogen" DESTINATIONS ALL 'udp://example.com:9090'
// CREATE SUBSCRIPTION "sub0" ON "mydb"."autogen" DESTINATIONS ANY 'udp://h1.example.com:9090', 'udp://h2.example.com:9090'
// CREATE USER "jdoe" WITH PASSWORD '1337password'
// CREATE USER "jdoe" WITH PASSWORD '1337password' WITH ALL PRIVILEGES

// DELETE FROM "cpu"
// DELETE FROM "cpu" WHERE time < '2000-01-01T00:00:00Z'
// DELETE WHERE time < '2000-01-01T00:00:00Z'

// DROP CONTINUOUS QUERY "myquery" ON "mydb"
// DROP DATABASE "mydb"
// DROP MEASUREMENT "cpu"
// DROP RETENTION POLICY "1h.cpu" ON "mydb"
// DROP SERIES FROM "cpu" WHERE cpu = 'cpu8'
// DROP SERIES FROM "telegraf".."cpu" WHERE cpu = 'cpu8'
// DROP SERIES FROM "telegraf"."autogen"."cpu" WHERE cpu = 'cpu8'
// DROP SHARD 1
// DROP SUBSCRIPTION "sub0" ON "mydb"."autogen"
// DROP USER "jdoe"

// GRANT ALL TO "jdoe"
// GRANT READ ON "mydb" TO "jdoe"
// REVOKE ALL PRIVILEGES FROM "jdoe"
// REVOKE READ ON "mydb" FROM "jdoe"
// KILL QUERY 36
// KILL QUERY 53 ON "myhost:8088"

// SELECT mean("value") INTO "cpu_1h".:MEASUREMENT FROM /cpu.*/
// SELECT mean("value") FROM "cpu" GROUP BY region, time(1d) fill(0) tz('America/Chicago')

// SHOW CONTINUOUS QUERIES
// SHOW DATABASES
// SHOW DIAGNOSTICS
// SHOW FIELD KEY CARDINALITY
// SHOW FIELD KEY EXACT CARDINALITY ON mydb
// SHOW FIELD KEYS
// SHOW FIELD KEYS FROM "cpu"
// SHOW GRANTS FOR "jdoe"
// SHOW MEASUREMENT CARDINALITY
// SHOW MEASUREMENT EXACT CARDINALITY ON mydb
// SHOW MEASUREMENTS
// SHOW MEASUREMENTS WHERE "region" = 'uswest' AND "host" = 'serverA'
// SHOW MEASUREMENTS WITH MEASUREMENT =~ /h2o.*/
// SHOW QUERIES
// SHOW RETENTION POLICIES ON "mydb"
// SHOW SERIES FROM "cpu" WHERE cpu = 'cpu8'
// SHOW SERIES FROM "telegraf".."cpu" WHERE cpu = 'cpu8'
// SHOW SERIES FROM "telegraf"."autogen"."cpu" WHERE cpu = 'cpu8'
// SHOW SERIES CARDINALITY
// SHOW SERIES CARDINALITY ON mydb
// SHOW SERIES EXACT CARDINALITY
// SHOW SERIES EXACT CARDINALITY ON mydb
// SHOW SHARD GROUPS
// SHOW SHARDS
// SHOW STATS
// SHOW SUBSCRIPTIONS
// SHOW TAG KEY CARDINALITY
// SHOW TAG KEY EXACT CARDINALITY
// SHOW TAG KEYS
// SHOW TAG KEYS FROM "cpu"
// SHOW TAG KEYS FROM "cpu" WHERE "region" = 'uswest'
// SHOW TAG KEYS WHERE "host" = 'serverA'
// SHOW TAG VALUES WITH KEY = "region"
// SHOW TAG VALUES FROM "cpu" WITH KEY = "region"
// SHOW TAG VALUES WITH KEY !~ /.*c.*/
// SHOW TAG VALUES FROM "cpu" WITH KEY IN ("region", "host") WHERE "service" = 'redis'
// SHOW TAG VALUES CARDINALITY WITH KEY = "myTagKey"
// SHOW TAG VALUES EXACT CARDINALITY WITH KEY = "myTagKey"
// SHOW USERS

func TestGetDatabaseFromInfluxQL(t *testing.T) {
	assertDatabase(t, "ALTER RETENTION POLICY \"1h.cpu\" ON \"mydb\" DEFAULT", "mydb")
	assertDatabase(t, "ALTER RETENTION POLICY \"policy1\" ON \"somedb\" DURATION 1h REPLICATION 4", "somedb")
	assertDatabase(t, "CREATE DATABASE \"foo\"", "foo")
	assertDatabase(t, "CREATE DATABASE \"bar\" WITH DURATION 1d REPLICATION 1 SHARD DURATION 30m NAME \"myrp\"", "bar")
	assertDatabase(t, "CREATE DATABASE \"mydb\" WITH NAME \"myrp\"", "mydb")
	assertDatabase(t, "CREATE RETENTION POLICY \"10m.events\" ON \"somedb\" DURATION 60m REPLICATION 2 SHARD DURATION 30m", "somedb")
	assertDatabase(t, "CREATE SUBSCRIPTION \"sub0\" ON \"mydb\".\"autogen\" DESTINATIONS ALL 'udp://example.com:9090'", "mydb")
	assertDatabase(t, "CREATE SUBSCRIPTION \"sub0\" ON \"my.db\".autogen DESTINATIONS ALL 'udp://example.com:9090'", "my.db")
	assertDatabase(t, "CREATE SUBSCRIPTION \"sub0\" ON mydb.autogen DESTINATIONS ALL 'udp://example.com:9090'", "mydb")
	assertDatabase(t, "CREATE SUBSCRIPTION \"sub0\" ON mydb.\"autogen\" DESTINATIONS ALL 'udp://example.com:9090'", "mydb")

	assertDatabase(t, "DROP CONTINUOUS QUERY \"myquery\" ON \"mydb\"", "mydb")
	assertDatabase(t, "DROP DATABASE \"mydb\"", "mydb")
	assertDatabase(t, "DROP RETENTION POLICY \"1h.cpu\" ON \"mydb\"", "mydb")
	assertDatabase(t, "DROP SUBSCRIPTION \"sub0\" ON \"mydb\".\"autogen\"", "mydb")
	assertDatabase(t, "GRANT READ ON \"mydb\" TO \"jdoe\"", "mydb")
	assertDatabase(t, "REVOKE READ ON \"mydb\" FROM \"jdoe\"", "mydb")
	assertDatabase(t, "SHOW FIELD KEY EXACT CARDINALITY ON mydb", "mydb")
	assertDatabase(t, "SHOW MEASUREMENT EXACT CARDINALITY ON mydb", "mydb")
	assertDatabase(t, "SHOW RETENTION POLICIES ON \"mydb\"", "mydb")
	assertDatabase(t, "SHOW SERIES CARDINALITY ON mydb", "mydb")
	assertDatabase(t, "SHOW SERIES EXACT CARDINALITY ON mydb", "mydb")

	assertDatabase(t, "CREATE DATABASE foo;", "foo")
	assertDatabase(t, "CREATE DATABASE \"f.oo\"", "f.oo")
	assertDatabase(t, "CREATE DATABASE \"f,oo\"", "f,oo")
	assertDatabase(t, "CREATE DATABASE \"f oo\"", "f oo")
	assertDatabase(t, "CREATE DATABASE \"f\\\"oo\"", "f\"oo")
}

func assertDatabase(t *testing.T, q string, d string) {
	qd, err := GetDatabaseFromInfluxQL(q)
	if err != nil {
		t.Errorf("error: %s, %s", q, err)
		return
	}
	if qd != d {
		t.Errorf("database wrong: %s, %s != %s", q, qd, d)
		return
	}
}

func TestGetMeasurementFromInfluxQL(t *testing.T) {
	assertMeasurement(t, "DELETE FROM \"cpu\"", "cpu")
	assertMeasurement(t, "DELETE FROM \"cpu\" WHERE time < '2000-01-01T00:00:00Z'", "cpu")

	assertMeasurement(t, "DROP MEASUREMENT cpu;", "cpu")
	assertMeasurement(t, "DROP MEASUREMENT \"cpu\"", "cpu")
	assertMeasurement(t, "DROP SERIES FROM \"cpu\" WHERE cpu = 'cpu8'", "cpu")
	assertMeasurement(t, "DROP SERIES FROM \"telegraf\"..\"cp u\" WHERE cpu = 'cpu8'", "cp u")
	assertMeasurement(t, "DROP SERIES FROM \"telegraf\".\"autogen\".\"cp u\" WHERE cpu = 'cpu8'", "cp u")

	assertMeasurement(t, "REVOKE ALL PRIVILEGES FROM \"jdoe\"", "jdoe")
	assertMeasurement(t, "REVOKE READ ON \"mydb\" FROM \"jdoe\"", "jdoe")

	assertMeasurement(t, "select * from cpu", "cpu")
	assertMeasurement(t, "(select *) from \"c.pu\"", "c.pu")
	assertMeasurement(t, "[select *] from \"c,pu\"", "c,pu")
	assertMeasurement(t, "{select *} from \"c pu\"", "c pu")
	assertMeasurement(t, "select * from \"cpu\"", "cpu")
	assertMeasurement(t, "select * from \"c\\\"pu\"", "c\"pu")
	assertMeasurement(t, "select * from 'cpu'", "cpu")
	// assertMeasurement(t, "select * from db.autogen.cpu", "cpu")
	// assertMeasurement(t, "select * from db.autogen.\"cpu.load\"", "cpu.load")
	// assertMeasurement(t, "select * from db.\"autogen\".\"cpu.load\"", "cpu.load")
	assertMeasurement(t, "select * from \"db\".\"autogen\".\"cpu.load\"", "cpu.load")
	assertMeasurement(t, "select * from \"d.b\".\"autogen\".\"cpu.load\"", "cpu.load")

	assertMeasurement(t, "SELECT mean(\"value\") INTO \"cpu\\\"_1h\".:MEASUREMENT FROM /cpu.*/", "/cpu.*/")
	assertMeasurement(t, "SELECT mean(\"value\") FROM \"cpu\" WHERE \"region\" = 'uswest' GROUP BY time(10m) fill(0)", "cpu")

	// assertMeasurement(t, "SHOW FIELD KEYS", "cpu")
	assertMeasurement(t, "SHOW FIELD KEYS FROM \"cpu\"", "cpu")
	assertMeasurement(t, "SHOW FIELD KEYS FROM \"1h\".\"cpu\"", "cpu")
	assertMeasurement(t, "SHOW FIELD KEYS FROM 1h.cpu", "cpu")
	assertMeasurement(t, "SHOW FIELD KEYS FROM \"cpu.load\"", "cpu.load")
	assertMeasurement(t, "SHOW FIELD KEYS FROM 1h.\"cpu.load\"", "cpu.load")
	assertMeasurement(t, "SHOW FIELD KEYS FROM \"1h\".\"cpu.load\"", "cpu.load")
	assertMeasurement(t, "SHOW SERIES FROM \"cpu\" WHERE cpu = 'cpu8'", "cpu")
	assertMeasurement(t, "SHOW SERIES FROM \"telegraf\"..\"cp.u\" WHERE cpu = 'cpu8'", "cp.u")
	assertMeasurement(t, "SHOW SERIES FROM \"telegraf\".\"autogen\".\"cp.u\" WHERE cpu = 'cpu8'", "cp.u")

	// assertMeasurement(t, "SHOW TAG KEYS", "cpu")
	assertMeasurement(t, "SHOW TAG KEYS FROM cpu", "cpu")
	assertMeasurement(t, "SHOW TAG KEYS FROM \"cpu\" WHERE \"region\" = 'uswest'", "cpu")
	// assertMeasurement(t, "SHOW TAG KEYS WHERE \"host\" = 'serverA'", "cpu")

	// assertMeasurement(t, "SHOW TAG VALUES WITH KEY = \"region\"", "cpu")
	assertMeasurement(t, "SHOW TAG VALUES FROM \"cpu\" WITH KEY = \"region\"", "cpu")
	// assertMeasurement(t, "SHOW TAG VALUES WITH KEY !~ /.*c.*/", "cpu")
	assertMeasurement(t, "SHOW TAG VALUES FROM \"cpu\" WITH KEY IN (\"region\", \"host\") WHERE \"service\" = 'redis'", "cpu")
}

func assertMeasurement(t *testing.T, q string, m string) {
	qm, err := GetMeasurementFromInfluxQL(q)
	if err != nil {
		t.Errorf("error: %s, %s", q, err)
		return
	}
	if qm != m {
		t.Errorf("measurement wrong: %s, %s != %s", q, qm, m)
		return
	}
}

func BenchmarkGetDatabaseFromInfluxQL(b *testing.B) {
	q := "CREATE SUBSCRIPTION \"sub0\" ON \"mydb\".\"autogen\" DESTINATIONS ALL 'udp://example.com:9090'"
	for i := 0; i < b.N; i++ {
		qd, err := GetDatabaseFromInfluxQL(q)
		if err != nil {
			b.Errorf("error: %s", err)
			return
		}
		if qd != "mydb" {
			b.Errorf("database wrong: %s != %s", qd, "mydb")
			return
		}
	}
}

func BenchmarkGetMeasurementFromInfluxQL(b *testing.B) {
	q := "SELECT mean(\"value\") FROM \"cpu\" WHERE \"region\" = 'uswest' GROUP BY time(10m) fill(0)"
	for i := 0; i < b.N; i++ {
		qm, err := GetMeasurementFromInfluxQL(q)
		if err != nil {
			b.Errorf("error: %s", err)
			return
		}
		if qm != "cpu" {
			b.Errorf("measurement wrong: %s != %s", qm, "cpu")
			return
		}
	}
}

func TestScanKey(t *testing.T) {
	tests := []struct {
		name string
		line []byte
		want string
	}{
		{
			name: "test1",
			line: []byte("cpu1,host=server02 value=0.67,id=2i,running=true,status=\"ok\""),
			want: "cpu1",
		},
		{
			name: "test2",
			line: []byte("cpu2,host=server04,region=us-west,direction=out running=false,status=\"fail\" 1596819659"),
			want: "cpu2",
		},
		{
			name: "test3",
			line: []byte("cpu3,host=server05,region=cn\\ north,tag\\ key=tag\\ value idle=64,system=1i,user=\"Dwayne Johnson\",admin=true"),
			want: "cpu3",
		},
		{
			name: "test4",
			line: []byte("cpu3,host=server06,region=cn\\ south,tag\\ key=value\\=with\"equals\" idle=16,system=16i,user=\"Jay Chou\",admin=false  439888"),
			want: "cpu3",
		},
		{
			name: "test5",
			line: []byte("cpu3,host=server07,region=cn\\ south,tag\\ key=value\\,with\"commas\" idle=74,system=23i,user=\"Stephen Chow\" 440204"),
			want: "cpu3",
		},
		{
			name: "test6",
			line: []byte("cpu4 idle=39,system=56i,user=\"Jay Chou\",brief\\ desc=\"the best \\\"singer\\\"\" 1422568543702"),
			want: "cpu4",
		},
		{
			name: "test7",
			line: []byte("\"measurement\\ with\\ spaces\\,\\ commas\\ and\\ \\\"quotes\\\"\",tag\\ key\\ with\\ equals\\ \\==tag\\,value\\,with\"commas\" field_k\\ey\\ with\\ \\==\"string field value, only \\\" need be escaped\""),
			want: "\"measurement with spaces, commas and \"quotes\"\"",
		},
	}
	for _, tt := range tests {
		got, err := ScanKey(tt.line)
		if err != nil || got != tt.want {
			t.Errorf("%v: got %v, want %v", tt.name, got, tt.want)
			continue
		}
	}
}

func TestLineToNano(t *testing.T) {
	tests := []struct {
		name string
		line []byte
		time bool
		unit string
		want string
	}{
		{
			name: "test1",
			line: []byte("cpu1 value=1,value2=2"),
			time: false,
			unit: "",
			want: "cpu1 value=1,value2=2",
		},
		{
			name: "test2",
			line: []byte(" cpu1 value=3,value2=4 1422568543702900257 "),
			time: true,
			unit: "",
			want: "cpu1 value=3,value2=4 1422568543702900257",
		},
		{
			name: "test3",
			line: []byte("  cpu2 value=1,value2=2 "),
			time: false,
			unit: "s",
			want: "cpu2 value=1,value2=2",
		},
		{
			name: "test4",
			line: []byte("cpu2 value=3,value2=4 1596819659"),
			time: true,
			unit: "s",
			want: "cpu2 value=3,value2=4 1596819659000000000",
		},
		{
			name: "test5",
			line: []byte("cpu3 value=1,value2=2"),
			time: false,
			unit: "h",
			want: "cpu3 value=1,value2=2",
		},
		{
			name: "test6",
			line: []byte(" cpu3 value=3,value2=4  439888  "),
			time: true,
			unit: "h",
			want: "cpu3 value=3,value2=4  1583596800000000000",
		},
		{
			name: "test7",
			line: []byte("  cpu4 value=1,value2=2  "),
			time: false,
			unit: "ms",
			want: "cpu4 value=1,value2=2",
		},
		{
			name: "test8",
			line: []byte("cpu4 value=3,value2=4  1596819420440"),
			time: true,
			unit: "ms",
			want: "cpu4 value=3,value2=4  1596819420440000000",
		},
		{
			name: "test9",
			line: []byte("\tcpu5 value=3,value2=4\t"),
			time: false,
			unit: "u",
			want: "cpu5 value=3,value2=4",
		},
		{
			name: "test10",
			line: []byte("\tcpu5 value=3,value2=4 1434055562000010\t"),
			time: true,
			unit: "u",
			want: "cpu5 value=3,value2=4 1434055562000010000",
		},
		{
			name: "test11",
			line: []byte(" \tcpu6 value=1,value2=2 \t"),
			time: false,
			unit: "ns",
			want: "cpu6 value=1,value2=2",
		},
		{
			name: "test12",
			line: []byte(" \tcpu6 value=3,value2=4 1434055562000010000 \t"),
			time: true,
			unit: "ns",
			want: "cpu6 value=3,value2=4 1434055562000010000",
		},
		{
			name: "test13",
			line: []byte("cpu6 value=5,value2=6 1434055562000010000"),
			time: true,
			unit: "n",
			want: "cpu6 value=5,value2=6 1434055562000010000",
		},
	}
	for _, tt := range tests {
		got := LineToNano(tt.line, tt.unit)
		if tt.time {
			if string(got) != tt.want {
				t.Errorf("%v: got %v, want %v", tt.name, string(got), tt.want)
				continue
			}
		} else {
			if strings.Index(string(got), tt.want) != 0 || len(string(got)) != len(tt.want)+20 {
				t.Errorf("%v: got %v, want %v", tt.name, string(got), tt.want)
				continue
			}
		}
	}
}

func TestCheckSpace(t *testing.T) {
	tests := []struct {
		name string
		line []byte
		want bool
	}{
		{
			name: "test1",
			line: []byte("cpu1,host=server02,region=us-west value=0.55,id=2i,running=true,status=\"ok\" 1422568543702900257"),
			want: true,
		},
		{
			name: "test2",
			line: []byte("cpu2,host=server04,region=us-west,direction=out running=false,status=\"fail\" 1596819659000000000"),
			want: true,
		},
		{
			name: "test3",
			line: []byte("cpu3,host=server05,region=cn\\ north,tag\\ key=tag\\ value idle=64,system=1i,user=\"Dwayne Johnson\",admin=true 1434055562000010000"),
			want: true,
		},
		{
			name: "test4",
			line: []byte("cpu3,host=server06,region=cn\\ south,tag\\ key=value\\=with\"equals\" idle=16,system=16i,user=\"Jay Chou\",admin=false  1422568543702900257"),
			want: true,
		},
		{
			name: "test5",
			line: []byte("cpu3,host=server07,region=cn\\ south,tag\\ key=value\\,with\"commas\" idle=74,system=23i,user=\"Stephen Chow\" 1583599143422568543"),
			want: true,
		},
		{
			name: "test6",
			line: []byte("cpu4 idle=39,system=56i,user=\"Jay Chou\",brief\\ desc=\"the best \\\"singer\\\"\" 1422568543702000000"),
			want: true,
		},
		{
			name: "test7",
			line: []byte("cpu4 idle=47,system=93i,user=\"Stephen Chow\",admin=true,brief\\ desc=\"the best \\\"novelist\\\"\"  1596819420440000000"),
			want: true,
		},
		{
			name: "test8",
			line: []byte("\"measurement\\ with\\ spaces\\,\\ commas\\ and\\ \\\"quotes\\\"\",tag\\ key\\ with\\ equals\\ \\==tag\\,value\\,with\"commas\" field_k\\ey\\ with\\ \\==\"string field value, only \\\" need be escaped\""),
			want: true,
		},
		{
			name: "test9",
			line: []byte("\"measurement\\ with\\ spaces\\,\\ commas\\ and\\ \\\"quotes\\\"\",tag\\ key\\ with\\ equals\\ \\==tag\\,value\\,with\"commas\" field_k\\ey\\ with\\ \\==\"string field value, multiple backslashes \\,\\\\,\\\\\\,\\\\\\\\\""),
			want: true,
		},
		{
			name: "test10",
			line: []byte("cpu  1422568543702900257"),
			want: false,
		},
		{
			name: "test11",
			line: []byte("cpu\\ idle  1422568543702900257"),
			want: false,
		},
		{
			name: "test12",
			line: []byte("cpu\\ idle,host=server02,region=us-west 1422568543702900257"),
			want: false,
		},
		{
			name: "test13",
			line: []byte("cpu,host=server02,region\\ area=us-west 1422568543702900257"),
			want: false,
		},
		{
			name: "test14",
			line: []byte("cpu,host=server02,region=us\\ west 1422568543702900257"),
			want: false,
		},
		{
			name: "test15",
			line: []byte("cpu,host=server02,region=us-west   1422568543702900257"),
			want: false,
		},
		{
			name: "test16",
			line: []byte("cpu,host=server02,region=us-west brief\\ value=0.93 1422568543702900257"),
			want: true,
		},
		{
			name: "test17",
			line: []byte("cpu,host=server02,region=us\\ west brief\\ value=0.93 1422568543702900257"),
			want: true,
		},
		{
			name: "test18",
			line: []byte("cpu,host=server02,region=us-west brief\\ desc=\"test server\" 1422568543702900257"),
			want: true,
		},
	}
	for _, tt := range tests {
		got := CheckSpace(tt.line)
		if got != tt.want {
			t.Errorf("%v: got %v, want %v", tt.name, got, tt.want)
			continue
		}
	}
}
