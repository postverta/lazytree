package fuse

import (
	"github.com/postverta/lazytree/tree"
	"github.com/hanwen/go-fuse/fuse"
	"log"
	"sync"
	"syscall"
	"time"
)

// Implementation of the fuse interface (go binding provided by go-fuse)
type FuseFileSystem struct {
	fuse.RawFileSystem

	// A big fat lock
	lock sync.RWMutex

	// Mapping from inode ID to node. Also serves as the node cache.
	// Note the root's inode is always 1
	inodeToNode map[uint64]tree.Node
	inodeRef    map[uint64]int32

	// Reverse mapping from node ID to inode
	nodeIdToInode map[string]uint64

	// Next available inode
	nextInode uint64

	// Next available file descriptor
	nextFd uint64

	// For directory entry streaming
	fdToDirEntryList map[uint64][]fuse.DirEntry

	// Constants for fuse entry timeouts. They don't matter really,
	// as we (will) use the low level APIs to invalidate entries.
	entryTimeout time.Duration
	attrTimeout  time.Duration

	// Keep the low level fuse server here
	fuseServer *fuse.Server
}

func NewFuseFileSystem(root tree.Node) *FuseFileSystem {
	return &FuseFileSystem{
		inodeToNode:      map[uint64]tree.Node{1: root},
		inodeRef:         map[uint64]int32{1: 1},
		nodeIdToInode:    map[string]uint64{root.Id(): 1},
		nextInode:        2,
		nextFd:           1,
		fdToDirEntryList: make(map[uint64][]fuse.DirEntry),
		entryTimeout:     100 * time.Millisecond,
		attrTimeout:      100 * time.Millisecond,
	}
}

// Helper functions

func (fs *FuseFileSystem) fillTimestampFromTime(ns int64, sec *uint64, nsec *uint32) {
	*sec = uint64(ns / 1e9)
	*nsec = uint32(ns % 1e9)
}

func (fs *FuseFileSystem) getNodeByInode(inode uint64) tree.Node {
	fs.lock.RLock()
	defer fs.lock.RUnlock()
	return fs.inodeToNode[inode]
}

func (fs *FuseFileSystem) getInodeByNode(node tree.Node, increaseRef bool) uint64 {
	nodeId := node.Id()
	fs.lock.Lock()
	defer fs.lock.Unlock()
	inode, found := fs.nodeIdToInode[nodeId]
	if !found {
		inode = fs.nextInode
		fs.nextInode++
		fs.nodeIdToInode[nodeId] = inode
		fs.inodeToNode[inode] = node
	}

	if increaseRef {
		fs.inodeRef[inode]++
	}

	return inode
}

func (fs *FuseFileSystem) fillAttr(header *fuse.InHeader, node tree.Node, attr *fuse.Attr) error {
	// We don't care about the owner of the file. Whoever calls this appears
	// to be the owner
	attr.Owner = header.Context.Owner

	switch node.Type() {
	case tree.NodeTypeDir:
		attr.Mode = 0777 | fuse.S_IFDIR
		attr.Size = 0
	case tree.NodeTypeFile:
		executable, err := node.GetExecutable()
		if err != nil {
			return err
		}

		if executable {
			attr.Mode = 0777 | fuse.S_IFREG
		} else {
			attr.Mode = 0666 | fuse.S_IFREG
		}

		attr.Size, err = node.GetSize()
		if err != nil {
			return err
		}
	case tree.NodeTypeSymlink:
		attr.Mode = 0666 | fuse.S_IFLNK
		attr.Size = 0
	}

	ctime, err := node.GetCreationTime()
	if err != nil {
		return err
	}
	mtime, err := node.GetModTime()
	if err != nil {
		return err
	}

	fs.fillTimestampFromTime(ctime.UnixNano(), &attr.Ctime, &attr.Ctimensec)
	fs.fillTimestampFromTime(mtime.UnixNano(), &attr.Mtime, &attr.Mtimensec)

	// does Nlink really matter?
	attr.Nlink = 1
	attr.Ino = fs.getInodeByNode(node, false)

	return nil
}

// Interface functions

func (fs *FuseFileSystem) String() string {
	return "LazyTree"
}

func (fs *FuseFileSystem) SetDebug(debug bool) {
	// Empty
}

func (fs *FuseFileSystem) Lookup(header *fuse.InHeader, name string, out *fuse.EntryOut) (status fuse.Status) {
	parent := fs.getNodeByInode(header.NodeId)
	if parent == nil {
		return fuse.ENOENT
	}

	if parent.Type() != tree.NodeTypeDir {
		log.Printf("[ERROR] lookup %s called on non-Directory node %d", name, header.NodeId)
		return fuse.ENOTDIR
	}

	child, err := parent.GetNodeByName(name)
	if err != nil {
		log.Printf("[ERROR] failed to get child node by name %s:%s", name, err.Error())
		return fuse.EIO
	}

	if child == nil {
		return fuse.ENOENT
	}

	inode := fs.getInodeByNode(child, true)
	out.NodeId = inode

	fs.fillTimestampFromTime(int64(fs.entryTimeout), &out.EntryValid, &out.EntryValidNsec)
	fs.fillTimestampFromTime(int64(fs.attrTimeout), &out.AttrValid, &out.AttrValidNsec)
	err = fs.fillAttr(header, child, &out.Attr)
	if err != nil {
		log.Printf("[ERROR] failed to fill the attr for node %d:%s", child.Id(), err.Error())
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *FuseFileSystem) Forget(nodeid, nlookup uint64) {
	fs.lock.Lock()
	defer fs.lock.Unlock()
	fs.inodeRef[nodeid] -= int32(nlookup)
	if fs.inodeRef[nodeid] < 0 {
		log.Panic("[PANIC] lookup count drops under zero")
	}
	if fs.inodeRef[nodeid] == 0 {
		delete(fs.inodeRef, nodeid)
		node := fs.inodeToNode[nodeid]
		delete(fs.inodeToNode, nodeid)
		if node != nil {
			delete(fs.nodeIdToInode, node.Id())
		}
	}
}

func (fs *FuseFileSystem) GetAttr(input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	node := fs.getNodeByInode(input.NodeId)
	if node == nil {
		return fuse.ENOENT
	}

	fs.fillTimestampFromTime(int64(fs.attrTimeout), &out.AttrValid, &out.AttrValidNsec)
	err := fs.fillAttr(&input.InHeader, node, &out.Attr)
	if err != nil {
		log.Printf("[ERROR] failed to fill the attr for node %d:%s", node.Id(), err.Error())
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *FuseFileSystem) SetAttr(input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	node := fs.getNodeByInode(input.NodeId)
	if node == nil {
		return fuse.ENOENT
	}

	if node.IsReadOnly() {
		return fuse.EPERM
	}

	if node.Type() == tree.NodeTypeFile {
		if input.Valid&fuse.FATTR_MODE != 0 {
			executable := false
			if input.Mode&0400 != 0 {
				executable = true
			}
			err := node.SetExecutable(executable)
			if err != nil {
				log.Printf("[ERROR] failed to set the executability for node %d:%s", node.Id(), err.Error())
				return fuse.EIO
			}
		}

		if input.Valid&fuse.FATTR_SIZE != 0 {
			err := node.Truncate(input.Size)
			if err != nil {
				log.Printf("[ERROR] failed to truncate for node %d:%s", node.Id(), err.Error())
				return fuse.EIO
			}
		}

		// Ignore other setAttr configurations
	}

	fs.fillTimestampFromTime(int64(fs.attrTimeout), &out.AttrValid, &out.AttrValidNsec)
	err := fs.fillAttr(&input.InHeader, node, &out.Attr)
	if err != nil {
		log.Printf("[ERROR] failed to fill the attr for node %d:%s", node.Id(), err.Error())
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *FuseFileSystem) Mknod(input *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	parent := fs.getNodeByInode(input.InHeader.NodeId)
	if parent == nil {
		return fuse.ENOENT
	}

	if parent.IsReadOnly() {
		return fuse.EPERM
	}

	// TODO: set isExecutable bit from input.Mode
	child, err := parent.CreateNode(name, tree.NodeTypeFile)
	if err != nil {
		if err == tree.ErrExist {
			return fuse.Status(syscall.EEXIST)
		} else {
			log.Printf("[ERROR] failed to create child file %s for node %d:%s", name, parent.Id(), err.Error())
			return fuse.EIO
		}
	}

	inode := fs.getInodeByNode(child, true)
	out.NodeId = inode

	fs.fillTimestampFromTime(int64(fs.entryTimeout), &out.EntryValid, &out.EntryValidNsec)
	fs.fillTimestampFromTime(int64(fs.attrTimeout), &out.AttrValid, &out.AttrValidNsec)
	err = fs.fillAttr(&input.InHeader, child, &out.Attr)
	if err != nil {
		log.Printf("[ERROR] failed to fill the attr for node %d:%s", child.Id(), err.Error())
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *FuseFileSystem) Mkdir(input *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	parent := fs.getNodeByInode(input.InHeader.NodeId)
	if parent == nil {
		return fuse.ENOENT
	}

	if parent.IsReadOnly() {
		return fuse.EPERM
	}

	child, err := parent.CreateNode(name, tree.NodeTypeDir)
	if err != nil {
		if err == tree.ErrExist {
			return fuse.Status(syscall.EEXIST)
		} else {
			log.Printf("[ERROR] failed to create child dir %s for node %d:%s", name, parent.Id(), err.Error())
			return fuse.EIO
		}
	}

	inode := fs.getInodeByNode(child, true)
	out.NodeId = inode

	fs.fillTimestampFromTime(int64(fs.entryTimeout), &out.EntryValid, &out.EntryValidNsec)
	fs.fillTimestampFromTime(int64(fs.attrTimeout), &out.AttrValid, &out.AttrValidNsec)
	err = fs.fillAttr(&input.InHeader, child, &out.Attr)
	if err != nil {
		log.Printf("[ERROR] failed to fill the attr for node %d:%s", child.Id(), err.Error())
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *FuseFileSystem) Unlink(header *fuse.InHeader, name string) (code fuse.Status) {
	parent := fs.getNodeByInode(header.NodeId)
	if parent == nil {
		return fuse.ENOENT
	}

	if parent.IsReadOnly() {
		return fuse.EPERM
	}

	err := parent.DeleteNode(name)
	if err != nil {
		if err == tree.ErrNotExist {
			return fuse.ENOENT
		} else {
			log.Printf("[ERROR] failed to delete child node %s for node %d:%s", name, parent.Id(), err.Error())
			return fuse.EIO
		}
	}

	return fuse.OK
}

func (fs *FuseFileSystem) Rmdir(header *fuse.InHeader, name string) (code fuse.Status) {
	parent := fs.getNodeByInode(header.NodeId)
	if parent == nil {
		return fuse.ENOENT
	}

	if parent.IsReadOnly() {
		return fuse.EPERM
	}

	childNode, err := parent.GetNodeByName(name)
	if err != nil {
		if err == tree.ErrNotExist {
			return fuse.ENOENT
		} else {
			log.Printf("[ERROR] failed to get child node %s for node %d:%s", name, parent.Id(), err.Error())
			return fuse.EIO
		}
	}

	if childNode.Type() != tree.NodeTypeDir {
		return fuse.ENOTDIR
	}

	// Only delete the node if the child dir is empty. This is important as
	// many apps rely on this behavior.
	grandChildren, err := childNode.GetNodes()
	if err != nil {
		log.Printf("[ERROR] failed to get children for node %d:%s", childNode.Id(), err.Error())
		return fuse.EIO
	}

	if len(grandChildren) == 0 {
		err := parent.DeleteNode(name)
		if err != nil {
			if err == tree.ErrNotExist {
				return fuse.ENOENT
			} else {
				log.Printf("[ERROR] failed to delete child node %s for node %d:%s", name, parent.Id(), err.Error())
				return fuse.EIO
			}
		}
	} else {
		return fuse.Status(syscall.ENOTEMPTY)
	}

	return fuse.OK
}

func (fs *FuseFileSystem) Rename(input *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	oldParent := fs.getNodeByInode(input.InHeader.NodeId)
	if oldParent == nil {
		return fuse.ENOENT
	}

	if oldParent.IsReadOnly() {
		return fuse.EPERM
	}

	newParent := fs.getNodeByInode(input.Newdir)
	if newParent == nil {
		return fuse.ENOENT
	}

	if newParent.IsReadOnly() {
		return fuse.EPERM
	}

	if oldParent.Type() != tree.NodeTypeDir {
		return fuse.ENOTDIR
	}

	if newParent.Type() != tree.NodeTypeDir {
		return fuse.ENOTDIR
	}

	child, err := oldParent.GetNodeByName(oldName)
	if err != nil {
		log.Printf("[ERROR] failed to find child node %s for node %d:%s", oldName, oldParent.Id(), err.Error())
	}

	if child == nil {
		return fuse.ENOENT
	}

	// Make before break (rename syscall expects overwrite behavior)
	err = newParent.AddNode(newName, child, true)
	if err != nil {
		if err == tree.ErrExist {
			return fuse.Status(syscall.EEXIST)
		} else {
			log.Printf("[ERROR] failed to add child node %s for node %d:%s", newName, oldParent.Id(), err.Error())
			return fuse.EIO
		}
	}

	err = oldParent.DeleteNode(oldName)
	if err != nil {
		if err == tree.ErrNotExist {
			return fuse.ENOENT
		} else {
			log.Printf("[ERROR] failed to delete child node %s for node %d:%s", newName, oldParent.Id(), err.Error())
			return fuse.EIO
		}
	}

	return fuse.OK
}

func (fs *FuseFileSystem) Link(input *fuse.LinkIn, filename string, out *fuse.EntryOut) (code fuse.Status) {
	child := fs.getNodeByInode(input.Oldnodeid)
	if child == nil {
		return fuse.ENOENT
	}
	parent := fs.getNodeByInode(input.NodeId)
	if parent == nil {
		return fuse.ENOENT
	}

	if parent.IsReadOnly() {
		return fuse.EPERM
	}

	err := parent.AddNode(filename, child, false)
	if err != nil {
		if err == tree.ErrExist {
			return fuse.Status(syscall.EEXIST)
		} else {
			log.Printf("[ERROR] failed to add child node %s for node %d:%s", filename, parent.Id(), err.Error())
			return fuse.EIO
		}
	}

	inode := fs.getInodeByNode(child, true)
	out.NodeId = inode

	fs.fillTimestampFromTime(int64(fs.entryTimeout), &out.EntryValid, &out.EntryValidNsec)
	fs.fillTimestampFromTime(int64(fs.attrTimeout), &out.AttrValid, &out.AttrValidNsec)
	err = fs.fillAttr(&input.InHeader, child, &out.Attr)
	if err != nil {
		log.Printf("[ERROR] failed to fill the attr for node %d:%s", child.Id(), err.Error())
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *FuseFileSystem) Symlink(header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) (code fuse.Status) {
	parent := fs.getNodeByInode(header.NodeId)
	if parent == nil {
		return fuse.ENOENT
	}

	if parent.IsReadOnly() {
		return fuse.EPERM
	}

	child, err := parent.CreateSymlink(linkName, pointedTo)
	if err != nil {
		if err == tree.ErrExist {
			return fuse.Status(syscall.EEXIST)
		} else {
			log.Printf("[ERROR] failed to create symlink %s for node %d:%s", linkName, parent.Id(), err.Error())
			return fuse.EIO
		}
	}

	inode := fs.getInodeByNode(child, true)
	out.NodeId = inode

	fs.fillTimestampFromTime(int64(fs.entryTimeout), &out.EntryValid, &out.EntryValidNsec)
	fs.fillTimestampFromTime(int64(fs.attrTimeout), &out.AttrValid, &out.AttrValidNsec)
	err = fs.fillAttr(header, child, &out.Attr)
	if err != nil {
		log.Printf("[ERROR] failed to fill the attr for node %d:%s", child.Id(), err.Error())
		return fuse.EIO
	}

	return fuse.OK
}

func (fs *FuseFileSystem) Readlink(header *fuse.InHeader) (out []byte, code fuse.Status) {
	node := fs.getNodeByInode(header.NodeId)
	if node == nil {
		return nil, fuse.ENOENT
	}

	if node.Type() != tree.NodeTypeSymlink {
		return nil, fuse.EINVAL
	}

	linkName, err := node.ReadLink()
	if err != nil {
		log.Printf("[ERROR] failed to read the symlink for node %d:%s", node.Id(), err.Error())
		return nil, fuse.EIO
	}
	return []byte(linkName), fuse.OK
}

func (fs *FuseFileSystem) Access(input *fuse.AccessIn) (code fuse.Status) {
	// Should we care about read-only at this point?
	return fuse.OK
}

func (fs *FuseFileSystem) GetXAttrSize(header *fuse.InHeader, attr string) (sz int, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *FuseFileSystem) GetXAttrData(header *fuse.InHeader, attr string) (data []byte, code fuse.Status) {
	return []byte{}, fuse.ENOSYS
}

func (fs *FuseFileSystem) ListXAttr(header *fuse.InHeader) (attributes []byte, code fuse.Status) {
	return []byte{}, fuse.ENOSYS
}

func (fs *FuseFileSystem) SetXAttr(input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	return fuse.ENOSYS
}

func (fs *FuseFileSystem) RemoveXAttr(header *fuse.InHeader, attr string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FuseFileSystem) Create(input *fuse.CreateIn, name string, out *fuse.CreateOut) (code fuse.Status) {
	parent := fs.getNodeByInode(input.NodeId)
	if parent == nil {
		return fuse.ENOENT
	}

	if parent.IsReadOnly() {
		return fuse.EPERM
	}

	child, err := parent.CreateNode(name, tree.NodeTypeFile)
	if err != nil {
		if err == tree.ErrExist {
			return fuse.Status(syscall.EEXIST)
		} else {
			log.Printf("[ERROR] failed to create child file %s for node %d:%s", name, parent.Id(), err.Error())
			return fuse.EIO
		}
	}

	inode := fs.getInodeByNode(child, true)
	out.NodeId = inode

	fs.fillTimestampFromTime(int64(fs.entryTimeout), &out.EntryValid, &out.EntryValidNsec)
	fs.fillTimestampFromTime(int64(fs.attrTimeout), &out.AttrValid, &out.AttrValidNsec)
	err = fs.fillAttr(&input.InHeader, child, &out.Attr)
	if err != nil {
		log.Printf("[ERROR] failed to fill the attr for node %d:%s", child.Id(), err.Error())
		return fuse.EIO
	}

	if child.IsReadOnly() && (input.Flags&(syscall.O_WRONLY|syscall.O_RDWR)) != 0 {
		return fuse.EPERM
	}

	fs.lock.Lock()
	defer fs.lock.Unlock()
	out.OpenOut.OpenFlags = input.Flags
	out.OpenOut.Fh = fs.nextFd
	fs.nextFd++

	return fuse.OK
}

func (fs *FuseFileSystem) Open(input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	node := fs.getNodeByInode(input.NodeId)
	if node == nil {
		return fuse.ENOENT
	}

	if node.Type() != tree.NodeTypeFile {
		return fuse.Status(syscall.EISDIR)
	}

	if node.IsReadOnly() && (input.Flags&(syscall.O_WRONLY|syscall.O_RDWR)) != 0 {
		return fuse.EPERM
	}

	fs.lock.Lock()
	defer fs.lock.Unlock()
	out.OpenFlags = input.Flags
	out.Fh = fs.nextFd
	fs.nextFd++

	return fuse.OK
}

func (fs *FuseFileSystem) Read(input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	node := fs.getNodeByInode(input.NodeId)
	if node == nil {
		return nil, fuse.ENOENT
	}

	if node.Type() != tree.NodeTypeFile {
		return nil, fuse.Status(syscall.EISDIR)
	}

	data, err := node.Read(input.Offset, uint32(len(buf)))
	if err != nil {
		log.Printf("[ERROR] failed to read from file %d:%s", node.Id(), err.Error())
		return nil, fuse.EIO
	}

	return fuse.ReadResultData(data), fuse.OK
}

func (fs *FuseFileSystem) Flock(input *fuse.FlockIn, flags int) (code fuse.Status) {
	// Not supporting file locking
	return fuse.ENOSYS
}

func (fs *FuseFileSystem) Release(input *fuse.ReleaseIn) {
	// We don't really care about file descriptors
}

func (fs *FuseFileSystem) Write(input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	node := fs.getNodeByInode(input.NodeId)
	if node == nil {
		return 0, fuse.ENOENT
	}

	if node.Type() != tree.NodeTypeFile {
		return 0, fuse.Status(syscall.EISDIR)
	}

	if node.IsReadOnly() {
		return 0, fuse.EPERM
	}

	written, err := node.Write(input.Offset, data)
	if err != nil {
		log.Printf("[ERROR] failed to read from file %d:%s", node.Id(), err.Error())
		return 0, fuse.EIO
	}

	return written, fuse.OK
}

func (fs *FuseFileSystem) Flush(input *fuse.FlushIn) fuse.Status {
	// Don't care.
	return fuse.OK
}

func (fs *FuseFileSystem) Fsync(input *fuse.FsyncIn) (code fuse.Status) {
	// We don't wanna rely on fsync call to ensure consistency.
	return fuse.OK
}

func (fs *FuseFileSystem) Fallocate(input *fuse.FallocateIn) (code fuse.Status) {
	// Not supported for now
	return fuse.ENOSYS
}

func (fs *FuseFileSystem) OpenDir(input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	node := fs.getNodeByInode(input.NodeId)
	if node == nil {
		return fuse.ENOENT
	}

	if node.Type() != tree.NodeTypeDir {
		return fuse.ENOTDIR
	}

	if node.IsReadOnly() && (input.Flags&(syscall.O_WRONLY|syscall.O_RDWR)) != 0 {
		return fuse.EPERM
	}

	// To ensure consistency of directory reading, we take a snapshot of the list of
	// nodes at open time, and associate it with the file descriptor
	childrenMap, err := node.GetNodes()
	if err != nil {
		log.Printf("[ERROR] failed to read children from directory %d:%s", node.Id(), err.Error())
		return fuse.EIO
	}

	dirEntryList := make([]fuse.DirEntry, len(childrenMap)+2)
	i := 0
	for name, child := range childrenMap {
		mode := fuse.S_IFREG
		if child.Type() == tree.NodeTypeDir {
			mode = fuse.S_IFDIR
		} else if child.Type() == tree.NodeTypeSymlink {
			mode = fuse.S_IFLNK
		}
		dirEntryList[i] = fuse.DirEntry{
			Mode: uint32(mode),
			Name: name,
			Ino:  fs.getInodeByNode(child, false),
		}
		i++
	}

	// Add the special "." and ".." entries
	dirEntryList[i] = fuse.DirEntry{
		Mode: fuse.S_IFDIR,
		Name: ".",
	}
	i++
	dirEntryList[i] = fuse.DirEntry{
		Mode: fuse.S_IFDIR,
		Name: "..",
	}

	fs.lock.Lock()
	out.OpenFlags = input.Flags
	out.Fh = fs.nextFd
	fs.nextFd++
	fs.fdToDirEntryList[out.Fh] = dirEntryList
	fs.lock.Unlock()

	return fuse.OK
}

func (fs *FuseFileSystem) ReadDir(input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	node := fs.getNodeByInode(input.NodeId)
	if node == nil {
		return fuse.ENOENT
	}

	if node.Type() != tree.NodeTypeDir {
		return fuse.ENOTDIR
	}

	// Read the directory entries from cache
	fs.lock.Lock()
	dirEntryList, found := fs.fdToDirEntryList[input.Fh]
	fs.lock.Unlock()
	if !found {
		return fuse.EINVAL
	}

	if input.Offset > uint64(len(dirEntryList)) {
		return fuse.EINVAL
	}

	todo := dirEntryList[input.Offset:]
	for _, entry := range todo {
		ok, _ := out.AddDirEntry(entry)
		if !ok {
			break
		}
	}

	return fuse.OK
}

func (fs *FuseFileSystem) ReadDirPlus(input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	node := fs.getNodeByInode(input.NodeId)
	if node == nil {
		return fuse.ENOENT
	}

	if node.Type() != tree.NodeTypeDir {
		return fuse.ENOTDIR
	}

	// Read the directory entries from cache
	fs.lock.Lock()
	dirEntryList, found := fs.fdToDirEntryList[input.Fh]
	fs.lock.Unlock()
	if !found {
		return fuse.EINVAL
	}

	if input.Offset > uint64(len(dirEntryList)) {
		return fuse.EINVAL
	}

	todo := dirEntryList[input.Offset:]
	for _, entry := range todo {
		entryDest, _ := out.AddDirLookupEntry(entry)
		if entryDest == nil {
			break
		}
		// Unknown Inode
		entryDest.Ino = uint64(0xffffffff)
		if entry.Name == "." || entry.Name == ".." {
			continue
		}

		*entryDest = fuse.EntryOut{}
		code := fs.Lookup(&input.InHeader, entry.Name, entryDest)
		if code != fuse.OK {
			return code
		}
	}

	return fuse.OK
}

func (fs *FuseFileSystem) ReleaseDir(input *fuse.ReleaseIn) {
	if input.Fh != 0 {
		fs.lock.Lock()
		delete(fs.fdToDirEntryList, input.Fh)
		fs.lock.Unlock()
	}
}

func (fs *FuseFileSystem) FsyncDir(input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.OK
}

func (fs *FuseFileSystem) StatFs(input *fuse.InHeader, out *fuse.StatfsOut) (code fuse.Status) {
	out.Blocks = 0xffffffff
	out.Bfree = 0xffffffff
	out.Bavail = 0xffffffff
	out.Files = 0
	out.Ffree = 0xffffffff
	out.Bsize = 8092 // does this matter?
	out.NameLen = 255
	out.Frsize = 0
	return fuse.OK
}

func (fs *FuseFileSystem) Init(*fuse.Server) {
}

// For testing
func (fs *FuseFileSystem) AssertEmpty() {
	// Making sure we are not leaking any inode after unmounting
	if len(fs.inodeToNode) != 0 {
		log.Panic("inodeToNode is not empty:", fs.inodeToNode)
	}

	if len(fs.inodeRef) != 0 {
		log.Panic("inodeRef is not empty:", fs.inodeRef)
	}

	if len(fs.nodeIdToInode) != 0 {
		log.Panic("nodeIdToInode is not empty:", fs.nodeIdToInode)
	}

	if len(fs.fdToDirEntryList) != 0 {
		log.Panic("fdToDirEntryList is not empty:", fs.fdToDirEntryList)
	}
}

// APIs
func (fs *FuseFileSystem) Mount(mountPoint string, debug bool) error {
	server, err := fuse.NewServer(fs, mountPoint, &fuse.MountOptions{
		Debug: debug,
		Options: []string{
			"noatime",
		},
	})
	if err != nil {
		return err
	}

	fs.fuseServer = server
	return nil
}

func (fs *FuseFileSystem) Serve() {
	if fs.fuseServer == nil {
		log.Panic("Serve is called before Mount!")
	}
	fs.fuseServer.Serve()
}

func (fs *FuseFileSystem) WaitMount() error {
	if fs.fuseServer == nil {
		log.Panic("WaitMount is called before Mount!")
	}
	return fs.fuseServer.WaitMount()
}

func (fs *FuseFileSystem) Unmount() error {
	if fs.fuseServer == nil {
		log.Panic("Unmount is called with mounting!")
	}
	return fs.fuseServer.Unmount()
}
