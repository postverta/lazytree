package mem

import (
	"encoding/json"
	"errors"
	"github.com/postverta/lazytree/driver"
	"github.com/postverta/lazytree/object"
	"github.com/postverta/lazytree/tree"
	"github.com/satori/go.uuid"
	"log"
	"sync"
	"time"
)

var (
	ErrNotImplemented = errors.New("method not implemented for the node type")
)

type Node interface {
	tree.Node

	Lock()
	Unlock()

	RLock()
	RUnlock()

	GetParent() Node
	SetParent(node Node)

	GetResolved() bool
	SetResolved(resolved bool)

	SetDriver(driver.Driver)
	SetReference(driver.Reference)

	// This method needs to be called with read lock held, similar to other
	// methods that access the internal state.
	Pack(outputChan chan *NodeData) error
}

// Mem-cached node implementation with optional persistent backend.
type NodeCommon struct {
	NodeId       string
	Parent       Node
	CreatedTime  time.Time
	ModifiedTime time.Time

	// External bit (this is true if a node used to be external, even
	// after its resolution)
	External  bool
	Driver    driver.Driver
	Reference driver.Reference

	Resolved bool

	Context *Context

	Mutex sync.RWMutex
}

func (n *NodeCommon) GetParent() Node {
	return n.Parent
}

func (n *NodeCommon) SetParent(node Node) {
	n.Parent = node
}

func (n *NodeCommon) GetResolved() bool {
	return n.Resolved
}

func (n *NodeCommon) SetResolved(resolved bool) {
	n.Resolved = resolved
}

func (n *NodeCommon) SetDriver(d driver.Driver) {
	n.Driver = d
}

func (n *NodeCommon) SetReference(ref driver.Reference) {
	n.Reference = ref
}

func (n *NodeCommon) Lock() {
	n.Mutex.Lock()
}

func (n *NodeCommon) Unlock() {
	n.Mutex.Unlock()
}

func (n *NodeCommon) RLock() {
	n.Mutex.RLock()
}

func (n *NodeCommon) RUnlock() {
	n.Mutex.RUnlock()
}

func (n *NodeCommon) Id() string {
	return n.NodeId
}

func (n *NodeCommon) IsReadOnly() bool {
	return false
}

func (n *NodeCommon) GetExecutable() (bool, error) {
	return false, ErrNotImplemented
}

func (n *NodeCommon) GetSize() (uint64, error) {
	return 0, ErrNotImplemented
}

func (n *NodeCommon) GetCreationTime() (time.Time, error) {
	return n.CreatedTime, nil
}

func (n *NodeCommon) GetModTime() (time.Time, error) {
	return n.ModifiedTime, nil
}

func (n *NodeCommon) GetNodes() (map[string]tree.Node, error) {
	return nil, ErrNotImplemented
}

func (n *NodeCommon) GetNodeByName(name string) (tree.Node, error) {
	return nil, ErrNotImplemented
}

func (n *NodeCommon) CreateNode(name string, nodeType tree.NodeType) (tree.Node, error) {
	return nil, ErrNotImplemented
}

func (n *NodeCommon) CreateSymlink(name string, target string) (tree.Node, error) {
	return nil, ErrNotImplemented
}

func (n *NodeCommon) AddNode(name string, node tree.Node, overwrite bool) error {
	return ErrNotImplemented
}

func (n *NodeCommon) DeleteNode(name string) error {
	return ErrNotImplemented
}

func (n *NodeCommon) SetExecutable(executable bool) error {
	return ErrNotImplemented
}

func (n *NodeCommon) Truncate(size uint64) error {
	return ErrNotImplemented
}

func (n *NodeCommon) Read(offset uint64, size uint32) ([]byte, error) {
	return nil, ErrNotImplemented
}

func (n *NodeCommon) Write(offset uint64, data []byte) (uint32, error) {
	return 0, ErrNotImplemented
}

func (n *NodeCommon) ReadLink() (string, error) {
	return "", ErrNotImplemented
}

func (n *NodeCommon) Pack(outputChan chan *NodeData) error {
	return ErrNotImplemented
}

// Helper functions
func (n *NodeCommon) packAsExternal() *NodeData {
	er := ExternalReference{
		DriverName:       n.Driver.Name(),
		EncodedReference: n.Reference.Encode(),
	}
	data, _ := json.Marshal(er)
	return &NodeData{
		Id:   n.Id(),
		Data: data,
	}
}

type File struct {
	NodeCommon
	IsExecutable bool

	// Obtained after resolution
	Data []byte
}

func (n *File) Type() tree.NodeType {
	return tree.NodeTypeFile
}

func (n *File) GetExecutable() (bool, error) {
	return n.IsExecutable, nil
}

func (n *File) GetSize() (uint64, error) {
	err := n.ensureResolved()
	if err != nil {
		return 0, err
	}

	n.RLock()
	defer n.RUnlock()
	return uint64(len(n.Data)), nil
}

func (n *File) SetExecutable(executable bool) error {
	n.Lock()
	defer n.Unlock()

	if n.IsExecutable != executable {
		n.ModifiedTime = time.Now()
		n.IsExecutable = executable
		if n.Context.Notifier != nil {
			n.Context.Notifier.NodeChanged(n.NodeId)
		}
	}
	return nil
}

func (n *File) Truncate(size uint64) error {
	err := n.ensureResolved()
	if err != nil {
		return err
	}

	n.Lock()
	defer n.Unlock()

	if uint64(len(n.Data)) != size {
		if uint64(len(n.Data)) > size {
			n.Data = n.Data[:size]
		} else {
			newData := make([]byte, size)
			copy(newData, n.Data)
			n.Data = newData
		}

		if n.Context.Notifier != nil {
			n.Context.Notifier.NodeChanged(n.NodeId)
		}

		n.ModifiedTime = time.Now()
	}

	return nil
}

func (n *File) Read(offset uint64, size uint32) ([]byte, error) {
	err := n.ensureResolved()
	if err != nil {
		return nil, err
	}

	n.RLock()
	defer n.RUnlock()

	end := offset + uint64(size)
	if end > uint64(len(n.Data)) {
		end = uint64(len(n.Data))
	}
	// make a copy of the data
	data := make([]byte, end-offset)
	copy(data, n.Data[offset:end])

	return data, nil
}

func (n *File) Write(offset uint64, data []byte) (uint32, error) {
	err := n.ensureResolved()
	if err != nil {
		return 0, err
	}

	n.Lock()
	defer n.Unlock()

	end := offset + uint64(len(data))
	if end > uint64(len(n.Data)) {
		newData := make([]byte, end)
		copy(newData, n.Data)
		n.Data = newData
	}

	copy(n.Data[offset:], data)

	if n.Context.Notifier != nil {
		n.Context.Notifier.NodeChanged(n.NodeId)
	}

	n.ModifiedTime = time.Now()
	return uint32(len(data)), nil
}

func (n *File) ensureResolved() error {
	n.RLock()
	if n.Resolved {
		n.RUnlock()
		return nil
	}
	n.RUnlock()

	n.Lock()
	defer n.Unlock()

	// Check again to see if things have changed
	if n.Resolved {
		return nil
	}

	obj, err := n.Driver.ResolveReference(object.ObjectTypeFile, n.Reference)
	if err != nil {
		return err
	}
	err = applyObjectToNode(obj, n)
	if err != nil {
		return err
	}
	n.Resolved = true

	if n.Context.Notifier != nil {
		n.Context.Notifier.NodeChanged(n.NodeId)
	}

	return nil
}

func (n *File) Pack(outputChan chan *NodeData) error {
	if !n.Resolved {
		nd := n.packAsExternal()
		outputChan <- nd
		return nil
	}

	nd := &NodeData{
		Id:   n.Id(),
		Data: make([]byte, len(n.Data)),
	}
	copy(nd.Data, n.Data)
	outputChan <- nd
	return nil
}

type Dir struct {
	NodeCommon

	// Obtained after resolution
	Nodes map[string]Node
}

func (n *Dir) Type() tree.NodeType {
	return tree.NodeTypeDir
}

func (n *Dir) GetNodes() (map[string]tree.Node, error) {
	err := n.ensureResolved()
	if err != nil {
		return nil, err
	}

	n.RLock()
	defer n.RUnlock()

	// make a copy of the node list
	nodes := make(map[string]tree.Node)
	for name, node := range n.Nodes {
		nodes[name] = node
	}

	return nodes, nil
}

func (n *Dir) GetNodeByName(name string) (tree.Node, error) {
	err := n.ensureResolved()
	if err != nil {
		return nil, err
	}

	n.RLock()
	defer n.RUnlock()

	node, found := n.Nodes[name]
	if found {
		return node, nil
	} else {
		return nil, nil
	}
}

func (n *Dir) CreateNode(name string, nodeType tree.NodeType) (tree.Node, error) {
	err := n.ensureResolved()
	if err != nil {
		return nil, err
	}

	n.Lock()
	defer n.Unlock()

	if _, found := n.Nodes[name]; found {
		return nil, tree.ErrExist
	}

	id := uuid.NewV4().String()
	var child Node
	if nodeType == tree.NodeTypeFile {
		child = NewFile(n.Context, id, false) // XXX isExecutable needs to be passed by API
	} else if nodeType == tree.NodeTypeDir {
		child = NewDir(n.Context, id)
	} else {
		log.Panic("Wrong type of node!")
	}

	child.SetParent(n)
	n.Nodes[name] = child

	if n.Context.Notifier != nil {
		n.Context.Notifier.NodeChanged(n.NodeId)
	}

	n.ModifiedTime = time.Now()
	return child, nil
}

func (n *Dir) CreateSymlink(name string, target string) (tree.Node, error) {
	err := n.ensureResolved()
	if err != nil {
		return nil, err
	}

	n.Lock()
	defer n.Unlock()

	if _, found := n.Nodes[name]; found {
		return nil, tree.ErrExist
	}

	nowTime := time.Now()
	child := NewSymlink(n.Context, uuid.NewV4().String(), target)
	child.SetParent(n)

	n.Nodes[name] = child

	if n.Context.Notifier != nil {
		n.Context.Notifier.NodeChanged(n.NodeId)
	}

	n.ModifiedTime = nowTime
	return child, nil
}

func (n *Dir) AddNode(name string, node tree.Node, overwrite bool) error {
	err := n.ensureResolved()
	if err != nil {
		return err
	}

	n.Lock()
	defer n.Unlock()

	if _, found := n.Nodes[name]; found && !overwrite {
		return tree.ErrExist
	}

	child, ok := node.(Node)
	if !ok {
		log.Panic("Not a mem node!")
	}
	child.SetParent(n)
	n.Nodes[name] = child

	if n.Context.Notifier != nil {
		n.Context.Notifier.NodeChanged(n.NodeId)
	}

	n.ModifiedTime = time.Now()
	return nil
}

func (n *Dir) DeleteNode(name string) error {
	err := n.ensureResolved()
	if err != nil {
		return err
	}

	n.Lock()
	defer n.Unlock()

	if _, found := n.Nodes[name]; !found {
		return tree.ErrNotExist
	}

	delete(n.Nodes, name)

	if n.Context.Notifier != nil {
		n.Context.Notifier.NodeChanged(n.NodeId)
	}

	n.ModifiedTime = time.Now()
	return nil
}

func (n *Dir) ensureResolved() error {
	n.RLock()
	if n.Resolved {
		n.RUnlock()
		return nil
	}
	n.RUnlock()

	n.Lock()
	defer n.Unlock()

	obj, err := n.Driver.ResolveReference(object.ObjectTypeTree, n.Reference)
	if err != nil {
		return err
	}
	err = applyObjectToNode(obj, n)
	if err != nil {
		return err
	}
	n.Resolved = true
	if n.Context.Notifier != nil {
		n.Context.Notifier.NodeChanged(n.NodeId)
	}

	return nil
}

func (n *Dir) Pack(outputChan chan *NodeData) error {
	if !n.Resolved {
		// save as an external object
		nd := n.packAsExternal()
		outputChan <- nd
		return nil
	}

	entries := make([]DirEntry, 0)
	for name, child := range n.Nodes {
		// Need to lock the child mostly because we need to store the
		// external bit in the parent. Otherwise, we might have
		// inconsistent DirEntry and the child node.

		child.RLock()
		// Right now we save resolved external node as normal node. In the
		// future we might optimize this to save resolved but unchanged
		// external node still as external node to save storage.
		external := !child.GetResolved()
		entry := DirEntry{
			Name:       name,
			Id:         child.Id(),
			NodeType:   child.Type(),
			IsExternal: external,
		}

		if child.Type() == tree.NodeTypeFile {
			file := child.(*File)
			entry.IsExecutable = file.IsExecutable
		}

		err := child.Pack(outputChan)
		if err != nil {
			child.RUnlock()
			return err
		}

		entries = append(entries, entry)
		child.RUnlock()
	}

	data, _ := json.Marshal(entries)
	nd := &NodeData{
		Id:   n.Id(),
		Data: data,
	}

	outputChan <- nd
	return nil
}

type Symlink struct {
	NodeCommon

	// Obtained after resolution
	Target string
}

func (n *Symlink) Type() tree.NodeType {
	return tree.NodeTypeSymlink
}

func (n *Symlink) ReadLink() (string, error) {
	err := n.ensureResolved()
	if err != nil {
		return "", err
	}

	return n.Target, nil
}

func (n *Symlink) ensureResolved() error {
	n.RLock()
	if n.Resolved {
		n.RUnlock()
		return nil
	}
	n.RUnlock()

	n.Lock()
	defer n.Unlock()

	obj, err := n.Driver.ResolveReference(object.ObjectTypeSymlink, n.Reference)
	if err != nil {
		return err
	}
	err = applyObjectToNode(obj, n)
	if err != nil {
		return err
	}
	n.Resolved = true
	if n.Context.Notifier != nil {
		n.Context.Notifier.NodeChanged(n.NodeId)
	}

	return nil
}

func (n *Symlink) Pack(outputChan chan *NodeData) error {
	if !n.Resolved {
		// save as an external object
		nd := n.packAsExternal()
		outputChan <- nd
		return nil
	}

	nd := &NodeData{
		Id:   n.Id(),
		Data: make([]byte, len([]byte(n.Target))),
	}
	copy(nd.Data, []byte(n.Target))
	outputChan <- nd
	return nil
}
