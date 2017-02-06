package tmpmysql

import (
	"testing"
)

func TestServer(t *testing.T) {
	mysql, err := NewServer()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("workDir: %s\n", mysql.workDir)
	if err := mysql.Destroy(); err != nil {
		t.Fatal(err)
	}
}
