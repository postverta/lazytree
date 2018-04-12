package git

import (
	"encoding/hex"
	"fmt"
	"github.com/postverta/lazytree/driver"
	"github.com/postverta/lazytree/object"
	"gopkg.in/src-d/go-git.v4"
	"gopkg.in/src-d/go-git.v4/config"
	"gopkg.in/src-d/go-git.v4/plumbing"
	"gopkg.in/src-d/go-git.v4/plumbing/filemode"
	"gopkg.in/src-d/go-git.v4/storage/memory"
	"io/ioutil"
	"log"
	"strings"
	"sync"
)

// Driver for git tree.
// The reference is of the format: "[GIT_URL] [GIT_REFSPEC] [ROOT_SHA1]"
// In the future with GVFS interface, we can get rid of the Refspec
// CAVEAT: as we do shallow clone (with depth=1), we only support the root on tip.
type Driver struct {
	mutex sync.Mutex
	// In-memory repos to store the objects. One for each Git URL.
	repos map[string]*git.Repository
}

func NewDriver() *Driver {
	return &Driver{
		repos: make(map[string]*git.Repository),
	}
}

type Reference struct {
	GitUrl  string
	GitRef  string
	GitHash [20]byte
}

func (r Reference) Encode() string {
	return fmt.Sprintf("%s %s %s",
		r.GitUrl,
		r.GitRef,
		hex.EncodeToString(r.GitHash[0:]))
}

func newObjFromHash(repo *git.Repository, gitHash [20]byte, objType object.ObjectType) (object.Object, error) {
	if objType == object.ObjectTypeTree {
		gitTreeObj, err := repo.TreeObject(gitHash)
		if err != nil {
			return nil, err
		}

		entries := make([]object.TreeEntry, 0)
		for _, gitTreeEntry := range gitTreeObj.Entries {
			if gitTreeEntry.Mode == filemode.Symlink || gitTreeEntry.Mode == filemode.Submodule {
				// No support for symlink and submodule yet
				continue
			}

			entryType := object.ObjectTypeFile
			if gitTreeEntry.Mode == filemode.Dir {
				entryType = object.ObjectTypeTree
			}
			entryMode := object.ObjectModeNormal
			if gitTreeEntry.Mode == filemode.Executable {
				entryMode = object.ObjectModeExecutable
			}
			childObj, err := newObjFromHash(repo, gitTreeEntry.Hash, entryType)
			if err != nil {
				return nil, err
			}
			entry := object.TreeEntry{
				Name:   gitTreeEntry.Name,
				Type:   entryType,
				Mode:   entryMode,
				Object: childObj,
			}
			entries = append(entries, entry)
		}
		return object.Tree{
			Entries: entries,
		}, nil
	} else if objType == object.ObjectTypeFile {
		gitBlobObj, err := repo.BlobObject(gitHash)
		if err != nil {
			return nil, err
		}

		reader, err := gitBlobObj.Reader()
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		data, err := ioutil.ReadAll(reader)
		if err != nil {
			return nil, err
		}

		return object.File{
			Data: data,
		}, nil
	} else {
		log.Panic("Wrong object type!")
		return nil, nil
	}
}

func (d *Driver) Name() string {
	return "git"
}

func (d *Driver) ResolveReference(objType object.ObjectType, reference interface{}) (object.Object, error) {
	ref, ok := reference.(Reference)
	if !ok {
		return nil, fmt.Errorf("Invalid reference %v", reference)
	}

	d.mutex.Lock()
	defer d.mutex.Unlock()
	repo, found := d.repos[ref.GitUrl]
	if found {
		// Making sure we have the right branches
		_, err := repo.Reference(plumbing.ReferenceName(ref.GitRef), false)
		if err == plumbing.ErrReferenceNotFound {
			// Fetch the reference
			err = repo.Fetch(&git.FetchOptions{
				RefSpecs: []config.RefSpec{
					config.RefSpec(fmt.Sprintf("%s:%s", ref.GitRef, ref.GitRef)),
				},
				Depth:    1,
				Progress: nil,
				Tags:     git.NoTags,
			})
			if err != nil {
				return nil, err
			}
		}
	} else {
		storage := memory.NewStorage()
		var err error
		repo, err = git.Clone(storage, nil, &git.CloneOptions{
			URL:           ref.GitUrl,
			ReferenceName: plumbing.ReferenceName(ref.GitRef),
			SingleBranch:  true,
			Depth:         1,
			Progress:      nil,
			Tags:          git.NoTags,
		})
		if err != nil {
			return nil, err
		}
	}

	return newObjFromHash(repo, ref.GitHash, objType)
}

func (d *Driver) DecodeReference(encodedRef string) (driver.Reference, error) {
	parts := strings.Split(encodedRef, " ")
	if len(parts) < 3 {
		return nil, fmt.Errorf("Cannot decode reference:%s", encodedRef)
	}
	var gitHash [20]byte
	_, err := hex.Decode(gitHash[0:], []byte(encodedRef))
	if err != nil {
		return nil, err
	}
	return Reference{
		GitUrl:  parts[0],
		GitRef:  parts[1],
		GitHash: gitHash,
	}, nil
}
