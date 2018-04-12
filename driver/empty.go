package driver

import (
	"github.com/postverta/lazytree/object"
	"log"
)

// A special empty driver to make sure things are sane even if we hit an unknown driver name.
type EmptyDriver struct{}

type EmptyReference string

func (e EmptyReference) Encode() string {
	return string(e)
}

func (ed *EmptyDriver) Name() string {
	return "empty"
}

func (ed *EmptyDriver) ResolveReference(objectType object.ObjectType, reference interface{}) (object.Object, error) {
	switch objectType {
	case object.ObjectTypeFile:
		return object.File{
			Data: []byte{},
		}, nil
	case object.ObjectTypeTree:
		return object.Tree{
			Entries: []object.TreeEntry{},
		}, nil
	case object.ObjectTypeSymlink:
		return object.Symlink{}, nil
	default:
		log.Panic("Wrong object type!")
		return nil, nil
	}
}

func (ed *EmptyDriver) DecodeReference(encodedRef string) (Reference, error) {
	return EmptyReference(""), nil
}
