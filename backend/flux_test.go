// Copyright 2021 Shiwen Cheng. All rights reserved.
// Use of this source code is governed by a MIT
// license that can be found in the LICENSE file.

package backend

import "testing"

func TestParseBucket(t *testing.T) {
	tests := []struct {
		name string
		have string
		want string
		werr error
	}{
		{
			name: "test1",
			have: `from(bucket: "example-bucket")`,
			want: "example-bucket",
		},
		{
			name: "test2",
			have: ` from ( bucket: 'example-bucket'' ) `,
			want: "example-bucket",
		},
		{
			name: "test3",
			have: ` from (  ) `,
			want: "",
			werr: ErrGetBucket,
		},
		{
			name: "test4",
			have: ` from  `,
			want: "",
			werr: ErrIllegalFluxQuery,
		},
		{
			name: "test5",
			have: `from(bucketID: "0261d8287f4d6000" )  `,
			want: "",
			werr: ErrGetBucket,
		},
		{
			name: "test6",
			have: `from(
    bucket: "example-bucket",
    host: "https://example.com",
    org: "example-org",
    token: "MySuP3rSecr3Tt0k3n",
)`,
			want: "example-bucket",
		},
	}
	for _, tt := range tests {
		got, err := ParseBucket(tt.have)
		if err != tt.werr || got != tt.want {
			t.Errorf("%v: got %v, %v, want %v, %v", tt.name, got, err, tt.want, tt.werr)
			continue
		}
	}
}

func TestParseMeasurement(t *testing.T) {
	tests := []struct {
		name string
		have string
		want string
		werr error
	}{
		{
			name: "test1",
			have: `filter(fn: (r) => r._measurement == "example-measurement")`,
			want: "example-measurement",
		},
		{
			name: "test2",
			have: `filter(fn: (r) => r._measurement == "example-measurement" and r._field == "example-field")`,
			want: "example-measurement",
		},
		{
			name: "test3",
			have: ` from (  ) `,
			want: "",
			werr: ErrGetMeasurement,
		},
		{
			name: "test4",
			have: ` filter  `,
			want: "",
			werr: ErrGetMeasurement,
		},
		{
			name: "test5",
			have: `filter(fn: (r) => r._measurement != "example-measurement")`,
			want: "",
			werr: ErrEqualMeasurement,
		},
		{
			name: "test6",
			have: `from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "measurement with spaces, commas and 'quotes'")`,
			want: `measurement with spaces, commas and 'quotes'`,
		},
		{
			name: "test7",
			have: `from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "'measurement with spaces, commas and 'quotes''")`,
			want: `'measurement with spaces, commas and 'quotes''`,
		},
		{
			name: "test8",
			have: `from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "measurement with spaces, commas and \"quotes\"")`,
			want: `measurement with spaces, commas and "quotes"`,
		},
		{
			name: "test9",
			have: `from(bucket:"mybucket") |> range(start:0) |> filter(fn: (r) => r._measurement == "\"measurement with spaces, commas and \"quotes\"\"")`,
			want: `"measurement with spaces, commas and "quotes""`,
		},
		{
			name: "test10",
			have: `from(bucket: "example-bucket")
    |> range(start: -1h)
    |> filter(fn: (r) => r._measurement == "example-measurement")
    |> filter(fn: (r) => r._field == "f0")
    |> yield(name: "filter-only")`,
			want: "example-measurement",
		},
		{
			name: "test11",
			have: `data = () => from(bucket: "example-bucket")
    |> range(start: -1h)
data() |> filter(fn: (r) => r._measurement == "m0")
data() |> filter(fn: (r) => r._measurement == "m1")`,
			want: "",
			werr: ErrMultiMeasurements,
		},
		{
			name: "test12",
			have: `from(bucket: "example-bucket")
|> range(start:-1d)
|> filter(fn: (r) => r["_measurement"] == "example-measurement")`,
			want: "example-measurement",
		},
	}
	for _, tt := range tests {
		got, err := ParseMeasurement(tt.have)
		if err != tt.werr || got != tt.want {
			t.Errorf("%v: got %v, %v, want %v, %v", tt.name, got, err, tt.want, tt.werr)
			continue
		}
	}
}
