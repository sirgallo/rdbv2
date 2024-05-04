package stats

import (
	"os"
	"syscall"
	"time"
	"github.com/sirgallo/logger"
)


var Log = logger.NewCustomLog(NAME)


func CalculateCurrentStats() (*Stats, error) {
	var stat syscall.Statfs_t
	var calcStatsErr error

	var path string
	path, calcStatsErr = os.Getwd()
	if calcStatsErr != nil { return nil, calcStatsErr }

	calcStatsErr = syscall.Statfs(path, &stat) 
	if calcStatsErr != nil {
		Log.Error(DISK_ERROR, path, ":", calcStatsErr.Error())
		return nil, calcStatsErr
	}

	blockSize := uint64(stat.Bsize)
	available := int64(stat.Bavail * blockSize)
	total := int64(stat.Blocks * blockSize)
	used := int64((stat.Blocks - stat.Bfree) * blockSize)
	currTime := time.Now()
	formattedTime := currTime.Format(time.RFC3339)
	
	return &Stats{
		AvailableDiskSpaceInBytes: available,
		TotalDiskSpaceInBytes: total,
		UsedDiskSpaceInBytes: used,
		Timestamp: formattedTime,
	}, nil
}