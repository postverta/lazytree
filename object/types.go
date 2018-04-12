package object

// Core types used for communicating between drivers and the file system
type ObjectType int

const (
	ObjectTypeTree ObjectType = iota
	ObjectTypeFile
	ObjectTypeSymlink
	ObjectTypeExternal
)

type ObjectMode int

const (
	ObjectModeNormal     ObjectMode = iota
	ObjectModeExecutable            // Only applies to type "file"
)

type Object interface {
	Type() ObjectType
}

type TreeEntry struct {
	Name   string
	Type   ObjectType
	Mode   ObjectMode
	Object Object
}

type Tree struct {
	Object
	Entries []TreeEntry
}

func (obj Tree) Type() ObjectType {
	return ObjectTypeTree
}

type File struct {
	Object
	Data []byte
}

func (obj File) Type() ObjectType {
	return ObjectTypeFile
}

type Symlink struct {
	Object

	Target string
}

func (obj Symlink) Type() ObjectType {
	return ObjectTypeSymlink
}

type External struct {
	Object

	// This is the type of the referred object
	RefType ObjectType

	DriverName       string
	EncodedReference string
}

func (obj External) Type() ObjectType {
	return ObjectTypeExternal
}
