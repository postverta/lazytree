package driver

import (
	"github.com/postverta/lazytree/object"
)

type Reference interface {
	Encode() string
}

// Common interfaces for all drivers to implement
type Driver interface {
	Name() string
	ResolveReference(objectType object.ObjectType, reference interface{}) (object.Object, error)
	DecodeReference(encodedReference string) (Reference, error)
}
