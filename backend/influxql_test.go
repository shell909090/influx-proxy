// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import "testing"

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
	assertDatabase(t, `ALTER RETENTION POLICY "1h.cpu" ON "mydb" DEFAULT`, "mydb")
	assertDatabase(t, `ALTER RETENTION POLICY "policy1" ON "somedb" DURATION 1h REPLICATION 4`, "somedb")
	assertDatabase(t, `CREATE DATABASE "foo"`, "foo")
	assertDatabase(t, `CREATE DATABASE "bar" WITH DURATION 1d REPLICATION 1 SHARD DURATION 30m NAME "myrp"`, "bar")
	assertDatabase(t, `CREATE DATABASE "mydb" WITH NAME "myrp"`, "mydb")
	assertDatabase(t, `CREATE RETENTION POLICY "10m.events" ON "somedb" DURATION 60m REPLICATION 2 SHARD DURATION 30m`, "somedb")
	assertDatabase(t, `CREATE SUBSCRIPTION "sub0" ON "mydb"."autogen" DESTINATIONS ALL 'udp://example.com:9090'`, "mydb")
	assertDatabase(t, `CREATE SUBSCRIPTION "sub0" ON "my.db".autogen DESTINATIONS ALL 'udp://example.com:9090'`, "my.db")
	assertDatabase(t, `CREATE SUBSCRIPTION "sub0" ON mydb.autogen DESTINATIONS ALL 'udp://example.com:9090'`, "mydb")
	assertDatabase(t, `CREATE SUBSCRIPTION "sub0" ON mydb."autogen" DESTINATIONS ALL 'udp://example.com:9090'`, "mydb")

	assertDatabase(t, `DROP CONTINUOUS QUERY "myquery" ON "mydb"`, "mydb")
	assertDatabase(t, `DROP DATABASE "mydb"`, "mydb")
	assertDatabase(t, `DROP RETENTION POLICY "1h.cpu" ON "mydb"`, "mydb")
	assertDatabase(t, `DROP SUBSCRIPTION "sub0" ON "mydb"."autogen"`, "mydb")
	assertDatabase(t, `GRANT READ ON "mydb" TO "jdoe"`, "mydb")
	assertDatabase(t, `REVOKE READ ON "mydb" FROM "jdoe"`, "mydb")
	assertDatabase(t, `SHOW FIELD KEY EXACT CARDINALITY ON mydb`, "mydb")
	assertDatabase(t, `SHOW MEASUREMENT EXACT CARDINALITY ON mydb`, "mydb")
	assertDatabase(t, `SHOW RETENTION POLICIES ON "mydb"`, "mydb")
	assertDatabase(t, `SHOW SERIES CARDINALITY ON mydb`, "mydb")
	assertDatabase(t, `SHOW SERIES EXACT CARDINALITY ON mydb`, "mydb")

	assertDatabase(t, `CREATE DATABASE foo;`, "foo")
	assertDatabase(t, `CREATE DATABASE "f.oo"`, "f.oo")
	assertDatabase(t, `CREATE DATABASE "f,oo"`, "f,oo")
	assertDatabase(t, `CREATE DATABASE "f oo"`, "f oo")
	assertDatabase(t, `CREATE DATABASE "f\"oo"`, "f\"oo")

	assertDatabase(t, `DROP SERIES FROM "telegraf".."cp u" WHERE cpu = 'cpu8'`, "telegraf")
	assertDatabase(t, `DROP SERIES FROM "telegraf"."autogen"."cp u" WHERE cpu = 'cpu8'`, "telegraf")

	assertDatabase(t, `select * from db..cpu`, "db")
	assertDatabase(t, `select * from db.autogen.cpu`, "db")
	assertDatabase(t, `select * from db."auto.gen".cpu`, "db")
	assertDatabase(t, `select * from test1.autogen."c\"pu.load"`, "test1")
	assertDatabase(t, `select * from test1."auto.gen"."c\"pu.load"`, "test1")
	assertDatabase(t, `select * from db."auto.gen"."cpu.load"`, "db")
	assertDatabase(t, `select * from "db"."autogen"."cpu.load"`, "db")
	assertDatabase(t, `select * from "d.b"."auto.gen"."cpu.load"`, "d.b")
	assertDatabase(t, `select * from "d\"b".."cpu.load"`, "d\"b")
	assertDatabase(t, `select * from "d.b".."cpu.load"`, "d.b")
	assertDatabase(t, `select * from "db".autogen.cpu`, "db")
	assertDatabase(t, `select * from "db"."auto.gen".cpu`, "db")
	assertDatabase(t, `select * from "d.b"..cpu`, "d.b")

	assertDatabase(t, `select * from "measurement with spaces, commas and 'quotes'"`, "")
	assertDatabase(t, `select * from "'measurement with spaces, commas and 'quotes''"`, "")
	assertDatabase(t, `select * from autogen."measurement with spaces, commas and 'quotes'"`, "")
	assertDatabase(t, `select * from "auto\"gen"."'measurement with spaces, commas and 'quotes''"`, "")
	assertDatabase(t, `select * from db1.."measurement with spaces, commas and 'quotes'"`, "db1")
	assertDatabase(t, `select * from "db\"1"."auto\"gen"."'measurement with spaces, commas and 'quotes''"`, "db\"1")
	assertDatabase(t, `select * from "measurement with spaces, commas and \"quotes\""`, "")
	assertDatabase(t, `select * from "\"measurement with spaces, commas and \"quotes\"\""`, "")
	assertDatabase(t, `select * from autogen."measurement with spaces, commas and \"quotes\""`, "")
	assertDatabase(t, `select * from "auto\"gen"."\"measurement with spaces, commas and \"quotes\"\""`, "")
	assertDatabase(t, `select * from db2.."measurement with spaces, commas and \"quotes\""`, "db2")
	assertDatabase(t, `select * from "db\"2"."auto\"gen"."\"measurement with spaces, commas and \"quotes\"\""`, "db\"2")

	assertDatabase(t, `select time, "/var/tmp", "D:\\work\\run\\log" from host1 order by desc limit 1`, "")
	assertDatabase(t, `select "time", "/var/tmp", "D:\\work\\run\\log" from "host1" order by desc limit 1`, "")

	assertDatabase(t, `select * from db..`, "db")
	assertDatabase(t, `select * from "db"..`, "db")
	assertDatabase(t, `select * from db.autogen`, "")
	assertDatabase(t, `select * from "db".autogen`, "")
	assertDatabase(t, `select * from db."auto.gen"`, "")
	assertDatabase(t, `select * from "db"."auto.gen"`, "")
	assertDatabase(t, `select * from db.`, "")
	assertDatabase(t, `select * from db`, "")
	assertDatabase(t, `select * from "d.b"`, "")
	assertDatabase(t, `select * from "d.b".`, "")
	assertDatabase(t, `select * from "db"`, "")

	assertDatabase(t, `select * from select_sth`, "")
	assertDatabase(t, `select * from "select sth"`, "")
	assertDatabase(t, `select * from db..select_sth`, "db")
	assertDatabase(t, `select * from db.rp."select sth"`, "db")
	assertDatabase(t, `select * from "select * from sth"`, "")
	assertDatabase(t, `select * from "(SELECT * FROM sth)"`, "")
	assertDatabase(t, `select * from db.."select * from sth"`, "db")
	assertDatabase(t, `select * from db.rp."(SELECT * FROM sth)"`, "db")

	assertDatabase(t, `SELECT SUM("max") FROM (SELECT MAX("water_level") FROM db."auto.gen"."h2o_feet" GROUP BY "location")`, "db")
	assertDatabase(t, `SELECT SUM("max") FROM ( SELECT MAX("water_level") FROM ( SELECT "water_total" / "water_unit" AS "water_level" FROM "db".autogen."pet_daycare" ) GROUP BY "location" )`, "db")
	assertDatabase(t, `select mean(kpi_3) from (select kpi_1+kpi_2 as kpi_3 from "d.b"..cpu where time < 1620877962) where time < 1620877962 group by time(1m),app`, "d.b")
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

func TestGetRetentionPolicyFromInfluxQL(t *testing.T) {
	assertRetentionPolicy(t, `DELETE FROM "cpu"`, "")
	assertRetentionPolicy(t, `DELETE FROM "cpu" WHERE time < '2000-01-01T00:00:00Z'`, "")

	assertRetentionPolicy(t, `DROP SERIES FROM "cpu" WHERE cpu = 'cpu8'`, "")
	assertRetentionPolicy(t, `DROP SERIES FROM "telegraf".."cp u" WHERE cpu = 'cpu8'`, "")
	assertRetentionPolicy(t, `DROP SERIES FROM "telegraf"."autogen"."cp u" WHERE cpu = 'cpu8'`, "autogen")

	assertRetentionPolicy(t, `select * from cpu`, "")
	assertRetentionPolicy(t, `(select *) from "c.pu"`, "")
	assertRetentionPolicy(t, `[select *] from "c,pu"`, "")
	assertRetentionPolicy(t, `{select *} from "c pu"`, "")
	assertRetentionPolicy(t, `select * from "cpu"`, "")
	assertRetentionPolicy(t, `select * from "c\"pu"`, "")
	assertRetentionPolicy(t, `select * from 'cpu'`, "")
	assertRetentionPolicy(t, `select * from autogen.cpu`, "autogen")
	assertRetentionPolicy(t, `select * from db..cpu`, "")
	assertRetentionPolicy(t, `select * from db.autogen.cpu`, "autogen")
	assertRetentionPolicy(t, `select * from db."auto.gen".cpu`, "auto.gen")
	assertRetentionPolicy(t, `select * from test1.autogen."c\"pu.load"`, "autogen")
	assertRetentionPolicy(t, `select * from test1."auto.gen"."c\"pu.load"`, "auto.gen")
	assertRetentionPolicy(t, `select * from db."auto.gen"."cpu.load"`, "auto.gen")
	assertRetentionPolicy(t, `select * from "db"."auto\"gen"."cpu.load"`, "auto\"gen")
	assertRetentionPolicy(t, `select * from "d.b"."auto.gen"."cpu.load"`, "auto.gen")
	assertRetentionPolicy(t, `select * from "db".."cpu.load"`, "")
	assertRetentionPolicy(t, `select * from "d.b".."cpu.load"`, "")
	assertRetentionPolicy(t, `select * from "db".autogen.cpu`, "autogen")
	assertRetentionPolicy(t, `select * from "db"."auto.gen".cpu`, "auto.gen")
	assertRetentionPolicy(t, `select * from "d.b"..cpu`, "")

	assertRetentionPolicy(t, `select * from "measurement with spaces, commas and 'quotes'"`, "")
	assertRetentionPolicy(t, `select * from "'measurement with spaces, commas and 'quotes''"`, "")
	assertRetentionPolicy(t, `select * from autogen."measurement with spaces, commas and 'quotes'"`, "autogen")
	assertRetentionPolicy(t, `select * from "auto\"gen"."'measurement with spaces, commas and 'quotes''"`, "auto\"gen")
	assertRetentionPolicy(t, `select * from db1.."measurement with spaces, commas and 'quotes'"`, "")
	assertRetentionPolicy(t, `select * from "db\"1"."auto\"gen"."'measurement with spaces, commas and 'quotes''"`, "auto\"gen")
	assertRetentionPolicy(t, `select * from "measurement with spaces, commas and \"quotes\""`, "")
	assertRetentionPolicy(t, `select * from "\"measurement with spaces, commas and \"quotes\"\""`, "")
	assertRetentionPolicy(t, `select * from autogen."measurement with spaces, commas and \"quotes\""`, "autogen")
	assertRetentionPolicy(t, `select * from "auto\"gen"."\"measurement with spaces, commas and \"quotes\"\""`, "auto\"gen")
	assertRetentionPolicy(t, `select * from db2.."measurement with spaces, commas and \"quotes\""`, "")
	assertRetentionPolicy(t, `select * from "db\"2"."auto\"gen"."\"measurement with spaces, commas and \"quotes\"\""`, "auto\"gen")

	assertRetentionPolicy(t, `select time, "/var/tmp", "D:\\work\\run\\log" from host1 order by desc limit 1`, "")
	assertRetentionPolicy(t, `select "time", "/var/tmp", "D:\\work\\run\\log" from "host1" order by desc limit 1`, "")

	assertRetentionPolicy(t, `SELECT mean("value") INTO "cpu\"_1h".:MEASUREMENT FROM /cpu.*/`, "")
	assertRetentionPolicy(t, `SELECT mean("value") FROM "cpu" WHERE "region" = 'uswest' GROUP BY time(10m) fill(0)`, "")

	assertRetentionPolicy(t, `select * from select_sth`, "")
	assertRetentionPolicy(t, `select * from "select sth"`, "")
	assertRetentionPolicy(t, `select * from db..select_sth`, "")
	assertRetentionPolicy(t, `select * from db.rp."select sth"`, "rp")
	assertRetentionPolicy(t, `select * from "select * from sth"`, "")
	assertRetentionPolicy(t, `select * from "(SELECT * FROM sth)"`, "")
	assertRetentionPolicy(t, `select * from db.."select * from sth"`, "")
	assertRetentionPolicy(t, `select * from db.rp."(SELECT * FROM sth)"`, "rp")

	assertRetentionPolicy(t, `SELECT SUM("max") FROM (SELECT MAX("water_level") FROM test1.autogen."h2o_feet" GROUP BY "location")`, "autogen")
	assertRetentionPolicy(t, `SELECT MEAN("difference") FROM ( SELECT "cats" - "dogs" AS "difference" FROM test1."auto.gen"."pet_daycare" )`, "auto.gen")
	assertRetentionPolicy(t, `SELECT "all_the_means" FROM (SELECT MEAN("water_level") AS "all_the_means" FROM db."auto.gen"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) ) WHERE "all_the_means" > 5`, "auto.gen")
	assertRetentionPolicy(t, `SELECT SUM("water_level_derivative") AS "sum_derivative" FROM (SELECT DERIVATIVE(MEAN("water_level")) AS "water_level_derivative" FROM "db"."auto\"gen"."h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),"location") GROUP BY "location"`, "auto\"gen")
	assertRetentionPolicy(t, `SELECT SUM("max") FROM ( SELECT MAX("water_level") FROM ( SELECT "water_total" / "water_unit" AS "water_level" FROM "d.b"."auto.gen"."pet_daycare" ) GROUP BY "location" )`, "auto.gen")
	assertRetentionPolicy(t, `select mean(kpi_3) from (select kpi_1+kpi_2 as kpi_3 from "db".autogen.cpu where time < 1620877962) where time < 1620877962 group by time(1m),app`, "autogen")
	assertRetentionPolicy(t, `select mean(kpi_3),max(kpi_3) FRoM (select kpi_1+kpi_2 as kpi_3 from "db"."auto.gen".cpu where time < 1620877962) where time < 1620877962 group by time(1m),app`, "auto.gen")

	assertRetentionPolicy(t, `SHOW FIELD KEYS`, "")
	assertRetentionPolicy(t, `SHOW FIELD KEYS FROM "cpu"`, "")
	assertRetentionPolicy(t, `SHOW FIELD KEYS FROM "1h"."cpu"`, "1h")
	assertRetentionPolicy(t, `SHOW FIELD KEYS FROM one_hour.cpu`, "one_hour")
	assertRetentionPolicy(t, `SHOW FIELD KEYS FROM "cpu.load"`, "")
	assertRetentionPolicy(t, `SHOW FIELD KEYS FROM one_hour."cpu.load"`, "one_hour")
	assertRetentionPolicy(t, `SHOW FIELD KEYS FROM "1h"."cpu.load"`, "1h")
	assertRetentionPolicy(t, `SHOW SERIES FROM "cpu" WHERE cpu = 'cpu8'`, "")
	assertRetentionPolicy(t, `SHOW SERIES FROM "telegraf".."cp.u" WHERE cpu = 'cpu8'`, "")
	assertRetentionPolicy(t, `SHOW SERIES FROM "telegraf"."autogen"."cp.u" WHERE cpu = 'cpu8'`, "autogen")

	assertRetentionPolicy(t, `SHOW TAG KEYS`, "")
	assertRetentionPolicy(t, `SHOW TAG KEYS FROM cpu`, "")
	assertRetentionPolicy(t, `SHOW TAG KEYS FROM "cpu" WHERE "region" = 'uswest'`, "")
	assertRetentionPolicy(t, `SHOW TAG KEYS WHERE "host" = 'serverA'`, "")

	assertRetentionPolicy(t, `SHOW TAG VALUES WITH KEY = "region"`, "")
	assertRetentionPolicy(t, `SHOW TAG VALUES FROM "cpu" WITH KEY = "region"`, "")
	assertRetentionPolicy(t, `SHOW TAG VALUES WITH KEY !~ /.*c.*/`, "")
	assertRetentionPolicy(t, `SHOW TAG VALUES FROM "cpu" WITH KEY IN ("region", "host") WHERE "service" = 'redis'`, "")
}

func assertRetentionPolicy(t *testing.T, q string, rp string) {
	qrp, err := GetRetentionPolicyFromInfluxQL(q)
	if err != nil && qrp != rp {
		t.Errorf("error: %s, %s", q, err)
		return
	}
	if qrp != rp {
		t.Errorf("retention policy wrong: %s, %s != %s", q, qrp, rp)
		return
	}
}

func TestGetMeasurementFromInfluxQL(t *testing.T) {
	assertMeasurement(t, `DELETE FROM "cpu"`, "cpu")
	assertMeasurement(t, `DELETE FROM "cpu" WHERE time < '2000-01-01T00:00:00Z'`, "cpu")

	assertMeasurement(t, `DROP MEASUREMENT cpu;`, "cpu")
	assertMeasurement(t, `DROP MEASUREMENT "cpu"`, "cpu")
	assertMeasurement(t, `DROP SERIES FROM "cpu" WHERE cpu = 'cpu8'`, "cpu")
	assertMeasurement(t, `DROP SERIES FROM "telegraf".."cp u" WHERE cpu = 'cpu8'`, "cp u")
	assertMeasurement(t, `DROP SERIES FROM "telegraf"."autogen"."cp u" WHERE cpu = 'cpu8'`, "cp u")

	assertMeasurement(t, `REVOKE ALL PRIVILEGES FROM "jdoe"`, "jdoe")
	assertMeasurement(t, `REVOKE READ ON "mydb" FROM "jdoe"`, "jdoe")

	assertMeasurement(t, `select * from cpu`, "cpu")
	assertMeasurement(t, `(select *) from "c.pu"`, "c.pu")
	assertMeasurement(t, `[select *] from "c,pu"`, "c,pu")
	assertMeasurement(t, `{select *} from "c pu"`, "c pu")
	assertMeasurement(t, `select * from "cpu"`, "cpu")
	assertMeasurement(t, `select * from "c\"pu"`, "c\"pu")
	assertMeasurement(t, `select * from 'cpu'`, "cpu")
	assertMeasurement(t, `select * from autogen.cpu`, "cpu")
	assertMeasurement(t, `select * from db..cpu`, "cpu")
	assertMeasurement(t, `select * from db.autogen.cpu`, "cpu")
	assertMeasurement(t, `select * from db."auto.gen".cpu`, "cpu")
	assertMeasurement(t, `select * from test1.autogen."c\"pu.load"`, "c\"pu.load")
	assertMeasurement(t, `select * from test1."auto.gen"."c\"pu.load"`, "c\"pu.load")
	assertMeasurement(t, `select * from db."auto.gen"."cpu.load"`, "cpu.load")
	assertMeasurement(t, `select * from "db"."autogen"."cpu.load"`, "cpu.load")
	assertMeasurement(t, `select * from "d.b"."auto.gen"."cpu.load"`, "cpu.load")
	assertMeasurement(t, `select * from "db".."cpu.load"`, "cpu.load")
	assertMeasurement(t, `select * from "d.b".."cpu.load"`, "cpu.load")
	assertMeasurement(t, `select * from "db".autogen.cpu`, "cpu")
	assertMeasurement(t, `select * from "db"."auto.gen".cpu`, "cpu")
	assertMeasurement(t, `select * from "d.b"..cpu`, "cpu")

	assertMeasurement(t, `select * from "measurement with spaces, commas and 'quotes'"`, "measurement with spaces, commas and 'quotes'")
	assertMeasurement(t, `select * from "'measurement with spaces, commas and 'quotes''"`, "'measurement with spaces, commas and 'quotes''")
	assertMeasurement(t, `select * from autogen."measurement with spaces, commas and 'quotes'"`, "measurement with spaces, commas and 'quotes'")
	assertMeasurement(t, `select * from "auto\"gen"."'measurement with spaces, commas and 'quotes''"`, "'measurement with spaces, commas and 'quotes''")
	assertMeasurement(t, `select * from db1.."measurement with spaces, commas and 'quotes'"`, "measurement with spaces, commas and 'quotes'")
	assertMeasurement(t, `select * from "db\"1"."auto\"gen"."'measurement with spaces, commas and 'quotes''"`, "'measurement with spaces, commas and 'quotes''")
	assertMeasurement(t, `select * from "measurement with spaces, commas and \"quotes\""`, "measurement with spaces, commas and \"quotes\"")
	assertMeasurement(t, `select * from "\"measurement with spaces, commas and \"quotes\"\""`, "\"measurement with spaces, commas and \"quotes\"\"")
	assertMeasurement(t, `select * from autogen."measurement with spaces, commas and \"quotes\""`, "measurement with spaces, commas and \"quotes\"")
	assertMeasurement(t, `select * from "auto\"gen"."\"measurement with spaces, commas and \"quotes\"\""`, "\"measurement with spaces, commas and \"quotes\"\"")
	assertMeasurement(t, `select * from db2.."measurement with spaces, commas and \"quotes\""`, "measurement with spaces, commas and \"quotes\"")
	assertMeasurement(t, `select * from "db\"2"."auto\"gen"."\"measurement with spaces, commas and \"quotes\"\""`, "\"measurement with spaces, commas and \"quotes\"\"")

	assertMeasurement(t, `select time, "/var/tmp", "D:\\work\\run\\log" from host1 order by desc limit 1`, "host1")
	assertMeasurement(t, `select "time", "/var/tmp", "D:\\work\\run\\log" from "host1" order by desc limit 1`, "host1")

	assertMeasurement(t, `SELECT mean("value") INTO "cpu\"_1h".:MEASUREMENT FROM /cpu.*/`, "/cpu.*/")
	assertMeasurement(t, `SELECT mean("value") FROM "cpu" WHERE "region" = 'uswest' GROUP BY time(10m) fill(0)`, "cpu")

	assertMeasurement(t, `select "time","metadata.share thoughts / metadata.share days." FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `select "time", "metadata.share thoughts / metadata.share days." FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `select "time" ,"metadata.share thoughts / metadata.share days." FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `select time,"metadata.share thoughts / metadata.share days." FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `select time, "metadata.share thoughts / metadata.share days." FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `select time ,"metadata.share thoughts / metadata.share days." FROM "h2o_feet"`, "h2o_feet")

	assertMeasurement(t, `select DISTINCT("level description"),INTEGRAL("water_level",1m) FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT BOTTOM("water_level",4),"location","level description" FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT FIRST("level description"),"location","water_level" FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT PERCENTILE("water_level",5),"location","level description" FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT ATAN2(MEAN("altitude_ft"), MEAN("distance_ft")) FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT ATAN2("altitude_ft", "distance_ft") FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT ELAPSED(/level/,1s) FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT LOG(MEAN("water_level"), 4) FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT MOVING_AVERAGE(MAX("water_level"),2) FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT "water_level"::float FROM "h2o_feet" LIMIT 4`, "h2o_feet")
	assertMeasurement(t, `SELECT "water_level"::integer,"water_level"::string FROM "h2o_feet" LIMIT 4`, "h2o_feet")
	assertMeasurement(t, `SELECT /<regular_expression_field_key>/ FROM "h2o_feet"`, "h2o_feet")

	assertMeasurement(t, `SELECT "A"+"B" FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT "A"-"B" FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT "A"*"B" FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT "A"/"B" FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT "A"%"B" FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT "A"&"B" FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT "A"|"B" FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT "A"^"B" FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT 100-"B" FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT "A"|5 FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT "B"%2 FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT 10 * ("A" - "B" - "C") FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT 10*/("A"+"B"+"C") FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT ("A" ^ true) & "B" FROM "h2o_feet"`, "h2o_feet")
	assertMeasurement(t, `SELECT ("A"^true)&"B" FROM "h2o_feet"`, "h2o_feet")

	assertMeasurement(t, `select * from select_sth`, "select_sth")
	assertMeasurement(t, `select * from "select sth"`, "select sth")
	assertMeasurement(t, `select * from db..select_sth`, "select_sth")
	assertMeasurement(t, `select * from db.rp."select sth"`, "select sth")
	assertMeasurement(t, `select * from "select * from sth"`, "select * from sth")
	assertMeasurement(t, `select * from "(SELECT * FROM sth)"`, "(SELECT * FROM sth)")
	assertMeasurement(t, `select * from db.."select * from sth"`, "select * from sth")
	assertMeasurement(t, `select * from db.rp."(SELECT * FROM sth)"`, "(SELECT * FROM sth)")

	assertMeasurement(t, `SELECT SUM("max") FROM (SELECT MAX("water_level") FROM "h2o_feet" GROUP BY "location")`, "h2o_feet")
	assertMeasurement(t, `SELECT MEAN("difference") FROM ( SELECT "cats" - "dogs" AS "difference" FROM "pet_daycare" )`, "pet_daycare")
	assertMeasurement(t, `SELECT "all_the_means" FROM (SELECT MEAN("water_level") AS "all_the_means" FROM "h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m) ) WHERE "all_the_means" > 5`, "h2o_feet")
	assertMeasurement(t, `SELECT SUM("water_level_derivative") AS "sum_derivative" FROM (SELECT DERIVATIVE(MEAN("water_level")) AS "water_level_derivative" FROM "h2o_feet" WHERE time >= '2015-08-18T00:00:00Z' AND time <= '2015-08-18T00:30:00Z' GROUP BY time(12m),"location") GROUP BY "location"`, "h2o_feet")
	assertMeasurement(t, `SELECT SUM("max") FROM ( SELECT MAX("water_level") FROM ( SELECT "water_total" / "water_unit" AS "water_level" FROM "pet_daycare" ) GROUP BY "location" )`, "pet_daycare")
	assertMeasurement(t, `select mean(kpi_3) from (select kpi_1+kpi_2 as kpi_3 from cpu where time < 1620877962) where time < 1620877962 group by time(1m),app`, "cpu")
	assertMeasurement(t, `select mean(kpi_3),max(kpi_3) FRoM (select kpi_1+kpi_2 as kpi_3 from cpu where time < 1620877962) where time < 1620877962 group by time(1m),app`, "cpu")

	assertMeasurement(t, `SHOW FIELD KEYS`, "")
	assertMeasurement(t, `SHOW FIELD KEYS FROM "cpu"`, "cpu")
	assertMeasurement(t, `SHOW FIELD KEYS FROM "1h"."cpu"`, "cpu")
	assertMeasurement(t, `SHOW FIELD KEYS FROM one_hour.cpu`, "cpu")
	assertMeasurement(t, `SHOW FIELD KEYS FROM "cpu.load"`, "cpu.load")
	assertMeasurement(t, `SHOW FIELD KEYS FROM one_hour."cpu.load"`, "cpu.load")
	assertMeasurement(t, `SHOW FIELD KEYS FROM "1h"."cpu.load"`, "cpu.load")
	assertMeasurement(t, `SHOW SERIES FROM "cpu" WHERE cpu = 'cpu8'`, "cpu")
	assertMeasurement(t, `SHOW SERIES FROM "telegraf".."cp.u" WHERE cpu = 'cpu8'`, "cp.u")
	assertMeasurement(t, `SHOW SERIES FROM "telegraf"."autogen"."cp.u" WHERE cpu = 'cpu8'`, "cp.u")

	assertMeasurement(t, `SHOW TAG KEYS`, "")
	assertMeasurement(t, `SHOW TAG KEYS FROM cpu`, "cpu")
	assertMeasurement(t, `SHOW TAG KEYS FROM "cpu" WHERE "region" = 'uswest'`, "cpu")
	assertMeasurement(t, `SHOW TAG KEYS WHERE "host" = 'serverA'`, "")

	assertMeasurement(t, `SHOW TAG VALUES WITH KEY = "region"`, "")
	assertMeasurement(t, `SHOW TAG VALUES FROM "cpu" WITH KEY = "region"`, "cpu")
	assertMeasurement(t, `SHOW TAG VALUES WITH KEY !~ /.*c.*/`, "")
	assertMeasurement(t, `SHOW TAG VALUES FROM "cpu" WITH KEY IN ("region", "host") WHERE "service" = 'redis'`, "cpu")
}

func assertMeasurement(t *testing.T, q string, m string) {
	qm, err := GetMeasurementFromInfluxQL(q)
	if err != nil && qm != m {
		t.Errorf("error: %s, %s", q, err)
		return
	}
	if qm != m {
		t.Errorf("measurement wrong: %s, %s != %s", q, qm, m)
		return
	}
}

func BenchmarkGetDatabaseFromInfluxQL(b *testing.B) {
	q := `CREATE SUBSCRIPTION "sub0" ON "mydb"."autogen" DESTINATIONS ALL 'udp://example.com:9090'`
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

func BenchmarkGetRetentionPolicyFromInfluxQL(b *testing.B) {
	q := `SELECT mean("value") FROM mydb."autogen"."cpu" WHERE "region" = 'uswest' GROUP BY time(10m) fill(0)`
	for i := 0; i < b.N; i++ {
		qrp, err := GetRetentionPolicyFromInfluxQL(q)
		if err != nil {
			b.Errorf("error: %s", err)
			return
		}
		if qrp != "autogen" {
			b.Errorf("retention policy wrong: %s != %s", qrp, "autogen")
			return
		}
	}
}

func BenchmarkGetMeasurementFromInfluxQL(b *testing.B) {
	q := `SELECT mean("value") FROM "cpu" WHERE "region" = 'uswest' GROUP BY time(10m) fill(0)`
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
