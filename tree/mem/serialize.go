package mem

import (
	"encoding/gob"
	"github.com/golang/snappy"
	"io"
)

// We use gob to encode each NodeData and use snappy to compress the final
// bytestream.

func Serialize(root *Dir, writer io.Writer) error {
	outputChan := make(chan *NodeData, 1024)
	go func() {
		root.RLock()
		defer root.RUnlock()
		root.Pack(outputChan)
		// Use a special nil NodeData to mark the end
		outputChan <- nil
	}()

	snappyWriter := snappy.NewBufferedWriter(writer)
	defer snappyWriter.Close()
	encoder := gob.NewEncoder(snappyWriter)
	failure := false
	var err error
	for {
		nd := <-outputChan
		if nd == nil {
			break
		}

		if failure {
			continue
		}

		err = encoder.Encode(nd)
		if err != nil {
			failure = true
			// still drain the outputChan to avoid blocking
		}
	}

	return err
}

func Deserialize(ctx *Context, reader io.Reader, external bool) (*Dir, error) {
	nodeDataMap := make(map[string]*NodeData)
	snappyReader := snappy.NewReader(reader)
	decoder := gob.NewDecoder(snappyReader)
	var rootNodeData *NodeData
	for {
		nd := &NodeData{}
		err := decoder.Decode(nd)
		if err != nil {
			if err != io.EOF {
				return nil, err
			} else {
				break
			}
		}
		nodeDataMap[nd.Id] = nd
		// Last node in the stream is the root
		rootNodeData = nd
	}

	return rootNodeData.UnpackDir(ctx, external, nodeDataMap)
}
