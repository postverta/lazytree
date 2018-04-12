package test

import (
	"github.com/postverta/lazytree/fuse"
	"github.com/postverta/lazytree/tree/mem"
	"github.com/satori/go.uuid"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"testing"
	"time"
)

var FuseFileSystems []*fuse.FuseFileSystem
var MountPoints []string

func MountFileSystem(rootDir *mem.Dir, mountPoint string, debug bool) error {
	fs := fuse.NewFuseFileSystem(rootDir)
	err := fs.Mount(mountPoint, debug)
	if err != nil {
		return err
	}

	go func() {
		fs.Serve()
	}()

	// Register the mountpoints and file systems for cleanup
	FuseFileSystems = append(FuseFileSystems, fs)
	MountPoints = append(MountPoints, mountPoint)
	return nil
}

// test for NPM install in the file system
func TestMain(m *testing.M) {
	ret := m.Run()

	// Clean up all mounts
	for _, fs := range FuseFileSystems {
		err := fs.Unmount()
		if err != nil {
			log.Panic("Cannot unmount fuse:", err)
		}
	}

	for _, mp := range MountPoints {
		err := os.Remove(mp)
		if err != nil {
			log.Panic("Cannot remove mount directory:", err)
		}
	}

	os.Exit(ret)
}

func TestNpm(t *testing.T) {
	// Mount an empty memfs
	mountPoint, err := ioutil.TempDir("", "npmtest")
	if err != nil {
		t.Fatal("Cannot create temp directory:", err)
	}
	ctx := &mem.Context{}
	rootDir := mem.NewDir(ctx, uuid.NewV4().String())
	err = MountFileSystem(rootDir, mountPoint, false)
	if err != nil {
		t.Fatal("Cannot mount file system:", err)
	}

	tarCmd := exec.Command("tar", "-zxf", "react.tgz", "-C", mountPoint)
	err = tarCmd.Run()
	if err != nil {
		t.Fatal("Cannot untar code:", err)
	}

	// First run one npm install to warm the cache
	npmCmd := exec.Command("npm", "install")
	npmCmd.Dir = mountPoint
	npmCmd.Stdout = os.Stdout
	npmCmd.Stderr = os.Stderr
	err = npmCmd.Run()
	if err != nil {
		t.Fatal("npm install failed:", err)
	}

	// Clean up the node_modules
	rmCmd := exec.Command("rm", "-rf", "node_modules")
	rmCmd.Dir = mountPoint
	err = rmCmd.Run()
	if err != nil {
		t.Fatal("rm files failed:", err)
	}

	// Run npm install again, and time it
	npmCmd2 := exec.Command("npm", "install")
	npmCmd2.Dir = mountPoint
	npmCmd2.Stdout = os.Stdout
	npmCmd2.Stderr = os.Stderr
	err = npmCmd2.Run()
	if err != nil {
		t.Fatal("npm install failed:", err)
	}

	// Run individual npm install/npm remove (cover the rmdir bug)
	npmInstallCmd := exec.Command("npm", "install", "superagent")
	npmInstallCmd.Dir = mountPoint
	npmInstallCmd.Stdout = os.Stdout
	npmInstallCmd.Stderr = os.Stderr
	err = npmInstallCmd.Run()
	if err != nil {
		t.Fatal("npm install superagent failded:", err)
	}

	npmRemoveCmd := exec.Command("npm", "remove", "superagent")
	npmRemoveCmd.Dir = mountPoint
	npmRemoveCmd.Stdout = os.Stdout
	npmRemoveCmd.Stderr = os.Stderr
	err = npmRemoveCmd.Run()
	if err != nil {
		t.Fatal("npm remove superagent failded:", err)
	}

	// Run the code
	nodeCmd := exec.Command("node", "index.js")
	nodeCmd.Dir = mountPoint
	nodeCmd.Env = []string{"APP_ROOT=" + mountPoint}
	nodeCmd.Stdout = os.Stdout
	nodeCmd.Stderr = os.Stderr
	err = nodeCmd.Run()
	if err != nil {
		t.Fatal("app running failed:", err)
	}

	// Save the file system to a file
	imageFile, err := ioutil.TempFile("", "npmtest")
	if err != nil {
		t.Fatal("Cannot open image file:", err)
	}
	fileName := imageFile.Name()
	startTime := time.Now()
	err = mem.Serialize(rootDir, imageFile)
	if err != nil {
		t.Fatal("Cannot serialize to disk:", err)
	}
	log.Printf("Writing to disk takes %fs\n", time.Since(startTime).Seconds())
	imageFile.Close()

	// Load the file system from the file
	imageFile, err = os.Open(fileName)
	if err != nil {
		t.Fatal("Cannot open image file:", err)
	}

	startTime = time.Now()
	rootDir2, err := mem.Deserialize(ctx, imageFile, false)
	if err != nil {
		t.Fatal("Cannot deserialize from disk:", err)
	}
	log.Printf("Reading from disk takes %fs\n", time.Since(startTime).Seconds())

	imageFile.Close()
	os.Remove(fileName)

	// Mount the loaded file system
	mountPoint2, err := ioutil.TempDir("", "npmtest")
	if err != nil {
		t.Fatal("Cannot create mountpoint:", err)
	}
	err = MountFileSystem(rootDir2, mountPoint2, false)
	if err != nil {
		t.Fatal("Cannot mount file system:", err)
	}

	// Make it more interesting, npm remove a package and install it later
	nodeRemoveCmd2 := exec.Command("npm", "remove", "express")
	nodeRemoveCmd2.Dir = mountPoint2
	nodeRemoveCmd2.Stdout = os.Stdout
	nodeRemoveCmd2.Stderr = os.Stderr
	err = nodeRemoveCmd2.Run()
	if err != nil {
		t.Fatal("npm remove express failed:", err)
	}

	nodeInstallCmd2 := exec.Command("npm", "install", "express")
	nodeInstallCmd2.Dir = mountPoint2
	nodeInstallCmd2.Stdout = os.Stdout
	nodeInstallCmd2.Stderr = os.Stderr
	err = nodeInstallCmd2.Run()
	if err != nil {
		t.Fatal("npm install express failed:", err)
	}

	// Try running the app, verify the file system is sane
	nodeCmd2 := exec.Command("node", "index.js")
	nodeCmd2.Dir = mountPoint2
	nodeCmd2.Env = []string{"APP_ROOT=" + mountPoint2}
	nodeCmd2.Stdout = os.Stdout
	nodeCmd2.Stderr = os.Stderr
	err = nodeCmd2.Run()
	if err != nil {
		t.Fatal("app running failed:", err)
	}
}
