package mem

import (
	"github.com/postverta/lazytree/driver"
	"time"
)

// New file node with empty data
func NewFile(ctx *Context, id string, isExecutable bool) *File {
	nowTime := time.Now()
	return &File{
		NodeCommon: NodeCommon{
			NodeId:       id,
			CreatedTime:  nowTime,
			ModifiedTime: nowTime,
			Resolved:     true,
			Context:      ctx,
		},
		IsExecutable: isExecutable,
		Data:         []byte{},
	}
}

// New file node with given data
func NewFileWithData(ctx *Context, id string, isExecutable bool, data []byte) *File {
	nowTime := time.Now()
	return &File{
		NodeCommon: NodeCommon{
			NodeId:       id,
			CreatedTime:  nowTime,
			ModifiedTime: nowTime,
			Resolved:     true,
			Context:      ctx,
		},
		IsExecutable: isExecutable,
		Data:         data,
	}
}

// New external file node with given driver and reference
func NewExternalFile(ctx *Context, id string, isExecutable bool, d driver.Driver, r driver.Reference) *File {
	nowTime := time.Now()
	return &File{
		NodeCommon: NodeCommon{
			NodeId:       id,
			CreatedTime:  nowTime,
			ModifiedTime: nowTime,
			External:     true,
			Context:      ctx,
			Driver:       d,
			Reference:    r,
		},
		IsExecutable: isExecutable,
	}
}

func NewDir(ctx *Context, id string) *Dir {
	nowTime := time.Now()
	return &Dir{
		NodeCommon: NodeCommon{
			NodeId:       id,
			CreatedTime:  nowTime,
			ModifiedTime: nowTime,
			Resolved:     true,
			Context:      ctx,
		},
		Nodes: make(map[string]Node),
	}
}

func NewDirWithChildren(ctx *Context, id string, nodes map[string]Node) *Dir {
	nowTime := time.Now()
	return &Dir{
		NodeCommon: NodeCommon{
			NodeId:       id,
			CreatedTime:  nowTime,
			ModifiedTime: nowTime,
			Resolved:     true,
			Context:      ctx,
		},
		Nodes: nodes,
	}
}

func NewExternalDir(ctx *Context, id string, d driver.Driver, r driver.Reference) *Dir {
	nowTime := time.Now()
	return &Dir{
		NodeCommon: NodeCommon{
			NodeId:       id,
			CreatedTime:  nowTime,
			ModifiedTime: nowTime,
			External:     true,
			Context:      ctx,
			Driver:       d,
			Reference:    r,
		},
	}
}

func NewSymlink(ctx *Context, id string, target string) *Symlink {
	nowTime := time.Now()
	return &Symlink{
		NodeCommon: NodeCommon{
			NodeId:       id,
			CreatedTime:  nowTime,
			ModifiedTime: nowTime,
			Resolved:     true,
			Context:      ctx,
		},
		Target: target,
	}
}

func NewExternalSymlink(ctx *Context, id string, d driver.Driver, r driver.Reference) *Symlink {
	nowTime := time.Now()
	return &Symlink{
		NodeCommon: NodeCommon{
			NodeId:       id,
			CreatedTime:  nowTime,
			ModifiedTime: nowTime,
			External:     true,
			Context:      ctx,
			Driver:       d,
			Reference:    r,
		},
	}
}
