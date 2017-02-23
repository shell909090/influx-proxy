// Copyright 2016 Eleme. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import "testing"

// SHOW USERS
// SHOW SUBSCRIPTIONS
// SHOW SHARDS
// SHOW SHARD GROUPS
// SHOW RETENTION POLICIES ON "mydb"
// SHOW DATABASES
// SHOW QUERIES
// SHOW MEASUREMENTS
// SHOW MEASUREMENTS WHERE "region" = 'uswest' AND "host" = 'serverA'
// SHOW MEASUREMENTS WITH MEASUREMENT =~ /h2o.*/
// SHOW GRANTS FOR "jdoe"
// SHOW CONTINUOUS QUERIES

// DROP USER "jdoe"
// DROP SUBSCRIPTION "sub0" ON "mydb"."autogen"
// DROP SHARD 1
// DROP DATABASE "mydb"
// DROP CONTINUOUS QUERY "myquery" ON "mydb"
// DROP RETENTION POLICY 1h.cpu ON mydb
// DROP MEASUREMENT "cpu"

// KILL QUERY 36
// DELETE WHERE time < '2000-01-01T00:00:00Z'
// GRANT ALL TO "jdoe"
// GRANT READ ON "mydb" TO "jdoe"

// CREATE USER "jdoe" WITH PASSWORD '1337password'
// CREATE USER "jdoe" WITH PASSWORD '1337password' WITH ALL PRIVILEGES
// CREATE SUBSCRIPTION "sub0" ON "mydb"."autogen" DESTINATIONS ALL 'udp://example.com:9090'
// CREATE SUBSCRIPTION "sub0" ON "mydb"."autogen" DESTINATIONS ANY 'udp://h1.example.com:9090', 'udp://h2.example.com:9090'
// CREATE RETENTION POLICY "10m.events" ON "somedb" DURATION 60m REPLICATION 2
// CREATE RETENTION POLICY "10m.events" ON "somedb" DURATION 60m REPLICATION 2 DEFAULT
// CREATE RETENTION POLICY "10m.events" ON "somedb" DURATION 60m REPLICATION 2 SHARD DURATION 30m
// CREATE DATABASE "foo"
// CREATE DATABASE "bar" WITH DURATION 1d REPLICATION 1 SHARD DURATION 30m NAME "myrp"
// CREATE DATABASE "mydb" WITH NAME "myrp"
// ALTER RETENTION POLICY "1h.cpu" ON "mydb" DEFAULT
// ALTER RETENTION POLICY "policy1" ON "somedb" DURATION 1h REPLICATION 4

func TestInfluxQL(t *testing.T) {
	checkPoint(t, "select * from cpu", "cpu")
	checkPoint(t, "(select *) from cpu", "cpu")
	checkPoint(t, "[select *] from cpu", "cpu")
	checkPoint(t, "{select *} from cpu", "cpu")
	checkPoint(t, "select * from \"cpu\"", "cpu")
	checkPoint(t, "select * from \"c\\\"pu\"", "c\"pu")
	checkPoint(t, "select * from 'cpu'", "cpu")

	checkPoint(t, "SELECT mean(\"value\") FROM \"cpu\" WHERE \"region\" = 'uswest' GROUP BY time(10m) fill(0)", "cpu")
	checkPoint(t, "SELECT mean(\"value\") INTO \"cpu\\\"_1h\".:MEASUREMENT FROM /cpu.*/", "/cpu.*/")

	checkPoint(t, "REVOKE ALL PRIVILEGES FROM \"jdoe\"", "jdoe")
	checkPoint(t, "REVOKE READ ON \"mydb\" FROM \"jdoe\"", "jdoe")

	checkPoint(t, "DELETE FROM \"cpu\"", "cpu")
	checkPoint(t, "DELETE FROM \"cpu\" WHERE time < '2000-01-01T00:00:00Z'", "cpu")

	// checkPoint(t, "DROP SERIES FROM \"telegraf\".\"autogen\".\"cpu\" WHERE cpu = 'cpu8'", "cpu")
	// checkPoint(t, "SHOW FIELD KEYS", "cpu")
	checkPoint(t, "SHOW FIELD KEYS FROM \"cpu\"", "cpu")
	// checkPoint(t, "SHOW SERIES FROM \"telegraf\".\"autogen\".\"cpu\" WHERE cpu = 'cpu8'", "cpu")

	// checkPoint(t, "SHOW TAG KEYS", "cpu")
	checkPoint(t, "SHOW TAG KEYS FROM \"cpu\"", "cpu")
	checkPoint(t, "SHOW TAG KEYS FROM \"cpu\" WHERE \"region\" = 'uswest'", "cpu")
	// checkPoint(t, "SHOW TAG KEYS WHERE \"host\" = 'serverA'", "cpu")

	// checkPoint(t, "SHOW TAG VALUES WITH KEY = \"region\"", "cpu")
	checkPoint(t, "SHOW TAG VALUES FROM \"cpu\" WITH KEY = \"region\"", "cpu")
	// checkPoint(t, "SHOW TAG VALUES WITH KEY !~ /.*c.*/", "cpu")
	checkPoint(t, "SHOW TAG VALUES FROM \"cpu\" WITH KEY IN (\"region\", \"host\") WHERE \"service\" = 'redis'", "cpu")
}

func checkPoint(t *testing.T, q string, m string) {
	qm, err := GetMeasurementFromInfluxQL(q)
	if err != nil {
		t.Errorf("error: %s", err)
		return
	}
	if qm != m {
		t.Errorf("measurement wrong: %s != %s", qm, m)
		return
	}
}

func BenchmarkInfluxQL(b *testing.B) {
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
