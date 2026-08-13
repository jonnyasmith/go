package cache

// syncDirectory is an explicit Windows fallback. Go's standard library cannot
// portably open and flush a directory handle, so file Sync followed by Rename
// is the strongest installation sequence available without a platform API.
func syncDirectory(string) error {
	return nil
}
