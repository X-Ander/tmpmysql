package tmpmysql

import (
	"database/sql"
	"testing"
)

func TestServer(t *testing.T) {
	mysql, err := NewServer()

	defer func() {
		if mysql != nil {
			if err := mysql.Destroy(); err != nil { t.Error(err) }
		}
	}()

	if err != nil { t.Fatal(err) }

	t.Logf("Work directory: %s\n", mysql.WorkDir)

	db, err := sql.Open("mysql", mysql.DSN)
	if err != nil { t.Fatal(err) }
	if db  == nil { t.Fatal("Can't connect to the database") }
	defer db.Close()

	if _, err := db.Exec("CREATE TABLE Users (" +
		"id INT NOT NULL, " +
		"name TEXT NOT NULL, " +
		"PRIMARY KEY (id)" +
		")"); err != nil { t.Fatal(err) }

	res, err := db.Exec("INSERT INTO Users (name) VALUES (\"Иван Иванов\")")
	if err != nil { t.Fatal(err) }

	n, err := res.RowsAffected()
	if err != nil { t.Fatal(err) }

	if n != 1 { t.Fatalf("One row insertion expected (got %d)", n) }

	id, err := res.LastInsertId()
	if err != nil { t.Fatal(err) }

	t.Logf("id = %d\n", id)

	rows, err := db.Query("SELECT name FROM Users WHERE id=?", id)
	if err != nil { t.Fatal(err) }
	defer rows.Close()
	cnt := 0
	for rows.Next() {
		cnt++
		var name string
		if err := rows.Scan(&name); err != nil { t.Fatal(err) }
		if name != "Иван Иванов" {
			t.Errorf("Expected \"Иван Иванов\", got \"%s\"", name)
		}
	}
	if cnt != 1 {
		t.Errorf("One row expected (got %d)", cnt)
	}

}
