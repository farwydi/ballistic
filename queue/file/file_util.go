package file

import "os"

// exists checks whether a file exists in the given path. It also fails if
// the path points to a directory or there is an error when trying to check the file.
func exists(path string) bool {
	info, err := os.Lstat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false
		}
		return false
	}
	if info.IsDir() {
		return false
	}
	return true
}
