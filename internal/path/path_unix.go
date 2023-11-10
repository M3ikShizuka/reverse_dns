//go:build linux

package path

var (
	logsDir = "/var/log/reversedns/logs"
)

func GetLogsDir() string {
	return logsDir
}
