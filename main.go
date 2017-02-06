package main

import (
	"database/sql"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type TempMySQL struct {
	dsn string
	workDir string
	pidFile string
	pid int
}

func NewTempMySQL() (*TempMySQL, error) {
	mysqld := findProgram("mysqld")
	if mysqld == "" {
		return nil, fmt.Errorf("Can't find the 'mysqld' program.")
	}
	mysql_install_db := findProgram("mysql_install_db")
	if mysql_install_db == "" {
		return nil, fmt.Errorf("Can't find the 'mysql_install_db' program.")
	}

	// Получим параметры, с которыми сервер MySQL запускается по умолчанию
	cmd := exec.Command(mysqld, "--print-defaults")
	defaultBytes, err := cmd.Output()
	if err != nil {
		return nil, err
	}
	defaults := string(defaultBytes)

	// Из этих параметров нам нужны baseDir и lc-messages-dir
	baseDir := extractDefault(defaults, "basedir")
	lcMessagesDir := extractDefault(defaults, "lc-messages-dir")

	var mysql TempMySQL

	// Временный рабочий каталог
	mysql.workDir, err = makeTempDir()
	if err != nil {
		return nil, err
	}

	dataDir := filepath.Join(mysql.workDir, "data")
	tempDir := filepath.Join(mysql.workDir, "tmp")
	socket  := filepath.Join(mysql.workDir, "sock")
	mysql.pidFile = filepath.Join(mysql.workDir, "pid")

	if err := os.Mkdir(dataDir, 0700); err != nil {
		return nil, err
	}
	if err := os.Mkdir(tempDir, 0700); err != nil {
		return nil, err
	}

	// Установим БД во временном каталоге
	cmd = exec.Command(mysql_install_db, "--no-defaults",
		"--basedir=" + baseDir,
		"--datadir=" + dataDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		log.Print(string(out))
		return nil, err
	}

	// Запустим сервер MySQL с временной БД
	cmd = exec.Command(mysqld, "--no-defaults", "--skip-networking",
		"--basedir="  + baseDir,
		"--lc-messages-dir=" + lcMessagesDir,
		"--datadir="  + dataDir,
		"--tmpdir="   + tempDir,
		"--pid-file=" + mysql.pidFile,
		"--socket="   + socket)
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Убедимся, что он действительно запустился
	for i := 0; i < 100; i++ {
		time.Sleep(100 * time.Millisecond)
		if exists(mysql.pidFile) {
			break
		}
	}
	if !exists(mysql.pidFile) {
		return nil, fmt.Errorf("Looks like mysqld has not started")
	}

	dsnRoot := "root@unix(" + socket + ")/"
	mysql.dsn = dsnRoot + "test"

	// Узнаем идентификатор процесса
	pidStr, err := ioutil.ReadFile(mysql.pidFile)
	if err != nil {
		return nil, err
	}
	if _, err := fmt.Sscanf(string(pidStr), "%d", &mysql.pid); err != nil {
		return nil, err
	}

	// Подключимся к нашему серверу
	db, err := sql.Open("mysql", dsnRoot + "mysql")
	if err != nil {
		return nil, err
	}

	// Создадим тестовую базу данных
	if _, err = db.Exec("CREATE DATABASE test CHARACTER SET utf8"); err != nil {
		return nil, err
	}

	return &mysql, nil
}

func (mysql *TempMySQL) Destroy() error {

	// Пошлём сигнал завершения серверу
	proc, err := os.FindProcess(mysql.pid)
	if err != nil {
		return err
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		return err
	}

	// Подождём, пока сервер завершит работу
	for i := 0; i < 100; i++ {
		time.Sleep(100 * time.Millisecond)
		if !exists(mysql.pidFile) {
			break
		}
	}
	if exists(mysql.pidFile) {
		return fmt.Errorf("Looks like mysqld does not want to stop")
	}

	// Удалим временный каталог
	if err := os.RemoveAll(mysql.workDir); err != nil {
		return err
	}

	return nil
}

// Из строки, которую возвращает mysqld --print-defaults, извлекает значение
// параметра с указанным именем
func extractDefault(defaults, name string) string {
	key := "--" + name + "="
	res := ""
	if i := strings.Index(defaults, key); i >= 0 {
		res = defaults[i + len(key):]
		if i := strings.Index(res, " "); i >= 0 {
			res = res[:i]
		}
	}
	return res
}

// Находит исполняемый файл указанной программы
func findProgram(name string) string {
	paths := strings.Split(os.Getenv("PATH"), ":")
	paths = append(paths,
		"/usr/local/sbin", "/usr/local/bin",
		"/usr/sbin", "/usr/bin", "/sbin", "/bin")
	for _, path := range paths {
		if file := filepath.Join(path, name); exists(file) {
			return file
		}
	}
	return ""
}

// Проверяет, существует ли указанный файл
func exists(file string) bool {
	_, err := os.Stat(file)
	return err == nil
}

// Создаёт временный каталог и возвращает путь к нему
func makeTempDir() (string, error) {
	tempRoot := os.TempDir()
	namePrefix := "tmpmysql_"
	for i := 0; i < 10000; i++ {
		dir := filepath.Join(tempRoot, fmt.Sprintf("%s%04d", namePrefix, i))
		if !exists(dir) {
			err := os.Mkdir(dir, 0700)
			if err != nil {
				return "", err
			}
			return dir, nil
		}
		i++
	}
	return "", fmt.Errorf("Too many temporary directories were already created")
}

func main() {
	mysql, err := NewTempMySQL()
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("workDir: %s\n", mysql.workDir)
	if err := mysql.Destroy(); err != nil {
		log.Fatal(err)
	}
}
