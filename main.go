package tmpmysql

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

// Структура со сведениями о сервере
type Server struct {
	DSN     string // строка, используемая для подключения к тестовой БД
	WorkDir string // путь ко временному рабочему каталогу
	PidFile string // путь к файлу с идентификатором процесса mysqld
	Pid     int    // сам этот идентификатор
}

// Запустить новый экземпляр временного сервера и создать на нём тестовую БД
func NewServer() (*Server, error) {

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

	var mysql Server

	// Временный рабочий каталог
	mysql.WorkDir, err = makeTempDir()
	if err != nil {
		return nil, err
	}

	dataDir := filepath.Join(mysql.WorkDir, "data")
	tempDir := filepath.Join(mysql.WorkDir, "tmp")
	socket := filepath.Join(mysql.WorkDir, "sock")
	mysql.PidFile = filepath.Join(mysql.WorkDir, "pid")

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
		"--pid-file=" + mysql.PidFile,
		"--socket="   + socket)
	if err := cmd.Start(); err != nil {
		return nil, err
	}

	// Убедимся, что он действительно запустился
	for i := 0; i < 100; i++ {
		time.Sleep(100 * time.Millisecond)
		if exists(mysql.PidFile) {
			break
		}
	}
	if !exists(mysql.PidFile) {
		return &mysql, fmt.Errorf("Looks like mysqld has not started")
	}

	dsnRoot := "root@unix(" + socket + ")/"
	mysql.DSN = dsnRoot + "test"

	// Узнаем идентификатор процесса
	pidStr, err := ioutil.ReadFile(mysql.PidFile)
	if err != nil {
		return &mysql, err
	}
	if _, err := fmt.Sscanf(string(pidStr), "%d", &mysql.Pid); err != nil {
		return &mysql, err
	}

	// Подключимся к нашему серверу
	db, err := sql.Open("mysql", dsnRoot+"mysql")
	if err != nil {
		return &mysql, err
	}

	// Создадим тестовую базу данных
	if _, err = db.Exec("CREATE DATABASE test CHARACTER SET utf8"); err != nil {
		return &mysql, err
	}

	// Отключимся от нашего сервера
	if err := db.Close(); err != nil {
		return &mysql, err
	}

	return &mysql, nil
}

// Остановить сервер и удалить созданные временные файлы и каталоги
func (mysql *Server) Destroy() error {

	if mysql.Pid != 0 {
		// Пошлём сигнал завершения серверу
		proc, err := os.FindProcess(mysql.Pid)
		if err != nil {
			return err
		}
		if err := proc.Signal(syscall.SIGTERM); err != nil {
			return err
		}

		// Подождём, пока сервер завершит работу
		for i := 0; i < 100; i++ {
			time.Sleep(100 * time.Millisecond)
			if !exists(mysql.PidFile) {
				break
			}
		}
		if exists(mysql.PidFile) {
			return fmt.Errorf("Looks like mysqld does not want to stop")
		}
	}

	// Удалим временный каталог
	if err := os.RemoveAll(mysql.WorkDir); err != nil {
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
