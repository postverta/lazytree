package mem

import (
	"github.com/postverta/lazytree/driver"
	"github.com/postverta/lazytree/object"
	"github.com/satori/go.uuid"
	"log"
)

func newNodeByTreeEntry(ctx *Context, entry *object.TreeEntry) (Node, error) {
	id := uuid.NewV4().String()
	var node Node
	switch entry.Type {
	case object.ObjectTypeFile:
		obj := entry.Object.(object.File)
		node = NewFileWithData(ctx, id, entry.Mode == object.ObjectModeExecutable, obj.Data)
	case object.ObjectTypeTree:
		obj := entry.Object.(object.Tree)
		children := make(map[string]Node)
		for _, ce := range obj.Entries {
			var err error
			children[ce.Name], err = newNodeByTreeEntry(ctx, &ce)
			if err != nil {
				return nil, err
			}
		}
		node = NewDirWithChildren(ctx, id, children)
		for _, child := range children {
			child.SetParent(node)
		}
	case object.ObjectTypeSymlink:
		obj := entry.Object.(object.Symlink)
		node = NewSymlink(ctx, id, obj.Target)
	case object.ObjectTypeExternal:
		obj := entry.Object.(object.External)
		d, err := driver.GetDriver(obj.DriverName)
		if err != nil {
			return nil, err
		}
		ref, err := d.DecodeReference(obj.EncodedReference)
		if err != nil {
			return nil, err
		}

		switch obj.RefType {
		case object.ObjectTypeFile:
			node = NewExternalFile(ctx, id, entry.Mode == object.ObjectModeExecutable, d, ref)
		case object.ObjectTypeTree:
			node = NewExternalDir(ctx, id, d, ref)
		case object.ObjectTypeSymlink:
			node = NewExternalSymlink(ctx, id, d, ref)
		}
	}
	return node, nil
}

// Recursively apply an object to a node (tree).
func applyObjectToNode(obj object.Object, node Node) error {
	if node.GetResolved() {
		log.Panic("Trying to apply data to an already resolved node!")
	}

	switch obj.Type() {
	case object.ObjectTypeFile:
		fileObj, ok := obj.(object.File)
		if !ok {
			log.Panicf("Wrong object type! %v", obj)
		}
		fileNode, ok := node.(*File)
		if !ok {
			log.Panic("Wrong node type!")
		}
		fileNode.Data = fileObj.Data
	case object.ObjectTypeTree:
		treeObj, ok := obj.(object.Tree)
		if !ok {
			log.Panic("Wrong object type!")
		}
		dirNode, ok := node.(*Dir)
		if !ok {
			log.Panic("Wrong node type!")
		}
		dirNode.Nodes = make(map[string]Node)
		for _, entry := range treeObj.Entries {
			childNode, err := newNodeByTreeEntry(dirNode.Context, &entry)
			if err != nil {
				return err
			}
			dirNode.Nodes[entry.Name] = childNode
			childNode.SetParent(dirNode)
		}
	case object.ObjectTypeSymlink:
		symlinkObj, ok := obj.(object.Symlink)
		if !ok {
			log.Panic("Wrong object type!")
		}
		symlinkNode, ok := node.(*Symlink)
		if !ok {
			log.Panic("Wrong node type!")
		}
		symlinkNode.Target = symlinkObj.Target
	default:
		log.Panic("Wrong object type!")
	}
	return nil
}
