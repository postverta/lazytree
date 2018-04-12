package mem

import (
	"encoding/json"
	"fmt"
	"github.com/postverta/lazytree/driver"
	"github.com/postverta/lazytree/tree"
)

// Serialized format of a node. Used to communicate with the persistent backend.
type NodeData struct {
	Id   string
	Data []byte
}

// Encode directoy entries. Stored as JSON in blob.
type DirEntry struct {
	Name         string
	Id           string
	NodeType     tree.NodeType
	IsExecutable bool
	IsExternal   bool
}

// Encode an external node. Stored as JSON in blob.
type ExternalReference struct {
	DriverName       string
	EncodedReference string
}

func (nd *NodeData) UnpackFile(ctx *Context, external bool, isExecutable bool) (*File, error) {
	if external {
		d, ref, err := nd.parseForExternal()
		if err != nil {
			return nil, err
		}
		return NewExternalFile(ctx, nd.Id, isExecutable, d, ref), nil
	} else {
		return NewFileWithData(ctx, nd.Id, isExecutable, nd.Data), nil
	}
}

func (nd *NodeData) UnpackDir(ctx *Context, external bool, nodeDataMap map[string]*NodeData) (*Dir, error) {
	if external {
		d, ref, err := nd.parseForExternal()
		if err != nil {
			return nil, err
		}
		return NewExternalDir(ctx, nd.Id, d, ref), nil
	} else {
		entries, err := nd.parseForDirEntries()
		if err != nil {
			return nil, err
		}

		children := make(map[string]Node)
		for _, e := range entries {
			childNd, found := nodeDataMap[e.Id]
			if !found {
				return nil, fmt.Errorf("Cannot find node %s in nodeDataMap", e.Id)
			}

			var childNode Node
			var err error
			switch e.NodeType {
			case tree.NodeTypeDir:
				childNode, err = childNd.UnpackDir(ctx, e.IsExternal, nodeDataMap)
			case tree.NodeTypeFile:
				childNode, err = childNd.UnpackFile(ctx, e.IsExternal, e.IsExecutable)
			case tree.NodeTypeSymlink:
				childNode, err = childNd.UnpackSymlink(ctx, e.IsExternal)
			}

			if err != nil {
				return nil, err
			}

			children[e.Name] = childNode
		}

		node := NewDirWithChildren(ctx, nd.Id, children)
		for _, child := range children {
			child.SetParent(node)
		}

		return node, nil
	}
}

func (nd *NodeData) UnpackSymlink(ctx *Context, external bool) (*Symlink, error) {
	if external {
		d, ref, err := nd.parseForExternal()
		if err != nil {
			return nil, err
		}
		return NewExternalSymlink(ctx, nd.Id, d, ref), nil
	} else {
		return NewSymlink(ctx, nd.Id, string(nd.Data)), nil
	}
}

// Helper
func (nd *NodeData) parseForDirEntries() ([]DirEntry, error) {
	var entries []DirEntry
	err := json.Unmarshal(nd.Data, &entries)
	if err != nil {
		return nil, err
	}
	return entries, nil
}

func (nd *NodeData) parseForExternal() (driver.Driver, driver.Reference, error) {
	er := ExternalReference{}
	err := json.Unmarshal(nd.Data, &er)
	if err != nil {
		return nil, nil, err
	}

	d, err := driver.GetDriver(er.DriverName)
	if err != nil {
		return nil, nil, err
	}

	ref, err := d.DecodeReference(er.EncodedReference)
	if err != nil {
		return nil, nil, err
	}

	return d, ref, nil
}
