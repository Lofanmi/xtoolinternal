package imports

import (
	"context"
	"fmt"
	"unsafe"

	"github.com/Lofanmi/cmap"
	"github.com/Lofanmi/xtoolinternal/gopathwalk"
)

// To find packages to import, the resolver needs to know about all of the
// the packages that could be imported. This includes packages that are
// already in modules that are in (1) the current module, (2) replace targets,
// and (3) packages in the module cache. Packages in (1) and (2) may change over
// time, as the client may edit the current module and locally replaced modules.
// The module cache (which includes all of the packages in (3)) can only
// ever be added to.
//
// The resolver can thus save state about packages in the module cache
// and guarantee that this will not change over time. To obtain information
// about new modules added to the module cache, the module cache should be
// rescanned.
//
// It is OK to serve information about modules that have been deleted,
// as they do still exist.
// TODO(suzmue): can we share information with the caller about
// what module needs to be downloaded to import this package?

type directoryPackageStatus int

const (
	_ directoryPackageStatus = iota
	directoryScanned
	nameLoaded
	exportsLoaded
)

type directoryPackageInfo struct {
	// status indicates the extent to which this struct has been filled in.
	status directoryPackageStatus
	// err is non-nil when there was an error trying to reach status.
	err error

	// Set when status >= directoryScanned.

	// dir is the absolute directory of this package.
	dir      string
	rootType gopathwalk.RootType
	// nonCanonicalImportPath is the package's expected import path. It may
	// not actually be importable at that path.
	nonCanonicalImportPath string

	// Module-related information.
	moduleDir  string // The directory that is the module root of this dir.
	moduleName string // The module name that contains this dir.

	// Set when status >= nameLoaded.

	packageName string // the package name, as declared in the source.

	// Set when status >= exportsLoaded.

	exports []string
}

// reachedStatus returns true when info has a status at least target and any error associated with
// an attempt to reach target.
func (info *directoryPackageInfo) reachedStatus(target directoryPackageStatus) (bool, error) {
	if info.err == nil {
		return info.status >= target, nil
	}
	if info.status == target {
		return true, info.err
	}
	return true, nil
}

// dirInfoCache is a concurrency safe map for storing information about
// directories that may contain packages.
//
// The information in this cache is built incrementally. Entries are initialized in scan.
// No new keys should be added in any other functions, as all directories containing
// packages are identified in scan.
//
// Other functions, including loadExports and findPackage, may update entries in this cache
// as they discover new things about the directory.
//
// The information in the cache is not expected to change for the cache's
// lifetime, using https://github.com/Lofanmi/cmap for better concurrent performance.
type dirInfoCache struct {
	// dirs stores information about packages in directories, keyed by absolute path.
	dirs      *cmap.Map[string, *directoryPackageInfo]
	listeners *cmap.Map[int, cacheListener]
}

type cacheListener func(directoryPackageInfo)

// newDirInfoCache creates a new dirInfoCache with initialized cmap instances
func newDirInfoCache() *dirInfoCache {
	return &dirInfoCache{
		dirs: cmap.New[string, *directoryPackageInfo](
			cmap.WithSerializer(defaultCMapSerializer),
		),
		listeners: cmap.New[int, cacheListener](
			cmap.WithSerializer(defaultCMapSerializer),
		),
	}
}

// ScanAndListen calls listener on all the items in the cache, and on anything
// newly added. The returned stop function waits for all in-flight callbacks to
// finish and blocks new ones.
func (d *dirInfoCache) ScanAndListen(ctx context.Context, listener cacheListener) func() {
	ctx, cancel := context.WithCancel(ctx)

	// Flushing out all the callbacks is tricky without knowing how many there
	// are going to be. Setting an arbitrary limit makes it much easier.
	const maxInFlight = 10
	sema := make(chan struct{}, maxInFlight)
	for i := 0; i < maxInFlight; i++ {
		sema <- struct{}{}
	}

	cookie := int(uintptr(unsafe.Pointer(new(int)))) // A unique ID we can use for the listener.

	// Get all current keys using cmap's Keys method
	keys := d.dirs.Keys()

	// Add the listener using cmap's Set method
	d.listeners.Put(cookie, func(info directoryPackageInfo) {
		select {
		case <-ctx.Done():
			return
		case <-sema:
		}
		listener(info)
		sema <- struct{}{}
	})

	stop := func() {
		cancel()
		d.listeners.Remove(cookie)
		for i := 0; i < maxInFlight; i++ {
			<-sema
		}
	}

	// Process the pre-existing keys.
	for _, k := range keys {
		select {
		case <-ctx.Done():
			return stop
		default:
		}
		if v, ok := d.Load(k); ok {
			listener(v)
		}
	}

	return stop
}

// Store stores the package info for dir.
func (d *dirInfoCache) Store(dir string, info directoryPackageInfo) {
	_, old := d.dirs.Get(dir)
	d.dirs.Put(dir, &info)

	if !old {
		// Notify all listeners
		for _, listener := range d.listeners.Values() {
			listener(info)
		}
	}
}

// Load returns a copy of the directoryPackageInfo for absolute directory dir.
func (d *dirInfoCache) Load(dir string) (directoryPackageInfo, bool) {
	info, ok := d.dirs.Get(dir)
	if !ok {
		return directoryPackageInfo{}, false
	}
	return *info, true
}

// Keys returns the keys currently present in d.
func (d *dirInfoCache) Keys() (keys []string) {
	return d.dirs.Keys()
}

func (d *dirInfoCache) CachePackageName(info directoryPackageInfo) (string, error) {
	if loaded, err := info.reachedStatus(nameLoaded); loaded {
		return info.packageName, err
	}
	if scanned, err := info.reachedStatus(directoryScanned); !scanned || err != nil {
		return "", fmt.Errorf("cannot read package name, scan error: %v", err)
	}
	info.packageName, info.err = packageDirToName(info.dir)
	info.status = nameLoaded
	d.Store(info.dir, info)
	return info.packageName, info.err
}

func (d *dirInfoCache) CacheExports(ctx context.Context, env *ProcessEnv, info directoryPackageInfo) (string, []string, error) {
	if reached, _ := info.reachedStatus(exportsLoaded); reached {
		return info.packageName, info.exports, info.err
	}
	if reached, err := info.reachedStatus(nameLoaded); reached && err != nil {
		return "", nil, err
	}
	info.packageName, info.exports, info.err = loadExportsFromFiles(ctx, env, info.dir, false)
	if info.err == context.Canceled || info.err == context.DeadlineExceeded {
		return info.packageName, info.exports, info.err
	}
	// The cache structure wants things to proceed linearly. We can skip a
	// step here, but only if we succeed.
	if info.status == nameLoaded || info.err == nil {
		info.status = exportsLoaded
	} else {
		info.status = nameLoaded
	}
	d.Store(info.dir, info)
	return info.packageName, info.exports, info.err
}
