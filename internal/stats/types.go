package stats


type Stats struct {
	AvailableDiskSpaceInBytes int64
	TotalDiskSpaceInBytes int64
	UsedDiskSpaceInBytes int64
	Timestamp string
}

type StatsError string


const NAME = "Stats"

const (
	DISK_ERROR StatsError = "disk error"
)