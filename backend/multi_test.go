package backend

import "testing"

func TestScanKey(t *testing.T) {
	var points []string = []string{
		"cpu,host=server01,region=uswest value=1 1434055562000000000",
		"cpu value=3,value2=4 1434055562000010000",
		"temper\\ ature,machine=unit42,type=assembly internal=32,external=100 1434055562000000035",
		"temper\\,ature,machine=unit143,type=assembly internal=22,external=130 1434055562005000035",
	}
	var keys []string = []string{
		"cpu",
		"cpu",
		"temper ature",
		"temper,ature",
	}

	var key string
	var err error
	for i, s := range points {
		key, err = ScanKey([]byte(s))
		if err != nil {
			t.Errorf("error: %s", err)
			return
		}
		if key != keys[i] {
			t.Errorf("quota test failed: %s, %s", key, keys[i])
			return
		}
	}

	return
}
