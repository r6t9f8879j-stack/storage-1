//go:build windows

package api

import (
	"golang.org/x/sys/windows"
)

// DiskFree returns free bytes on the volume containing path.
func DiskFree(path string) (int64, error) {
	p, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var freeBytesAvailableToCaller, totalBytes, totalFreeBytes uint64
	err = windows.GetDiskFreeSpaceEx(p, &freeBytesAvailableToCaller, &totalBytes, &totalFreeBytes)
	if err != nil {
		return 0, err
	}
	return int64(freeBytesAvailableToCaller), nil
}