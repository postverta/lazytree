package tree

import (
	"errors"
	"time"
)

var (
	ErrNotExist = errors.New("node doesn't exist")
	ErrExist    = errors.New("node already exists")
	ErrPerm     = errors.New("node is read-only")
)

type NodeType int

const (
	NodeTypeDir NodeType = iota
	NodeTypeFile
	NodeTypeSymlink
)

// The main tree node interface. All tree implementation should extend this.
type Node interface {
	// A unique ID of the node. Cannot be modified throughout the entire life of this node.
	Id() string

	// Type of the node
	Type() NodeType

	// Whether the node is read-only
	IsReadOnly() bool

	// Is the file executable (only work on files)
	GetExecutable() (bool, error)

	// Size of the file in bytes (doesn't work on directories)
	GetSize() (uint64, error)

	// Creation time
	GetCreationTime() (time.Time, error)

	// Modification time
	GetModTime() (time.Time, error)

	// Get the child nodes (only work on directories)
	GetNodes() (map[string]Node, error)

	// Get one child node by name (only work on directories)
	// returns nil, nil if the name cannot be found
	GetNodeByName(name string) (Node, error)

	// Create a new child node (only works on directories)
	// returns ErrExist if the name already exists
	CreateNode(name string, nodeType NodeType) (Node, error)

	// Create a new child symlink
	CreateSymlink(name string, target string) (Node, error)

	// Add an existing child node (only works on directories)
	// returns ErrExist if the name already exists, unless overwrite
	// is true.
	AddNode(name string, node Node, overwrite bool) error

	// Delete a child node
	// returns ErrNotExist if the name cannot be found
	DeleteNode(name string) error

	// General I/O functions
	// Set the executable bit
	SetExecutable(executable bool) error

	// Truncate the file node (only works on files)
	Truncate(size uint64) error

	// Read data (only works on files)
	// should only return error when I/O failed
	Read(offset uint64, size uint32) ([]byte, error)

	// Write data (only works on files)
	// should only return error when I/O failed
	Write(offset uint64, data []byte) (uint32, error)

	// Read symlink target (only works on symlinks)
	ReadLink() (string, error)
}
