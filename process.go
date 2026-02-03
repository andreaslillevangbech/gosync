package main

import (
	"os"
	"encoding/json"
	"sort"
	"path/filepath"
	"crypto/sha256"
	"encoding/hex"
)

// The Block (chunk of a file)
type Block struct {
	ID   string // The SHA-256 Hash of the raw data (e.g., "a5b3c...")
	Size int64  // Size in bytes
}

// DirEntry represents one item inside a folder
// Keeping metadata here makes it easy show show a file tree later
// Name is used for sorting when comparing trees
type DirEntry struct {
	Name string `json:"name"`
	Type string `json:"type"` // "file" or "dir"
	ID   string `json:"id"`   // The Merkle Root of this child
}

func ProcessDirectory(path string, db Store) (string, error) {
	// 1. Read the real directory from disk
	files, err := os.ReadDir(path)
	if err != nil {
		return "", err
	}

	var entries []DirEntry

	for _, f := range files {
		var childID string
		var childType string

		fullPath := filepath.Join(path, f.Name())

		if f.IsDir() {
			childType = "dir"
			// recursion
			childID, err = ProcessDirectory(fullPath, db)
			if err != nil { return "", err }
		} else {
			childType = "file"
			// If it's a file, do the Chungking express
			blocks, err := ChunkFile(fullPath, db)
			if err != nil { return "", err }
			
			var fileData []byte
			childID, fileData, err = CalculateFileID(blocks)
			if err != nil { return "", err }

			// DB insert, saving metadata
			err = db.SaveMetadata(childID, "file", fileData)
			if err != nil { return "", err }
		}
		entries = append(entries, DirEntry{
			Name: f.Name(),
			Type: childType,
			ID:   childID,
		})
	}

	dirID, dirData, err := CalculateDirID(entries)
	if err != nil { return "", err }

	// DB INSERT: save directory metadata
	if err := db.SaveMetadata(dirID, "dir", dirData); err != nil {
		return "", err
	}

	return dirID, nil
}

func CalculateDirID(entries []DirEntry) (string, []byte, error) {
	// 1. Sort the entries (CRITICAL STEP)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name < entries[j].Name
	})

	// 3. Serialize to JSON
	// Example: {"entries":[{"name":"a.txt","id":"..."}, {"name":"b.txt","id":"..."}]}
	data, err := json.Marshal(entries)
	if err != nil {
		return "", nil, err
	}

	sum := sha256.Sum256(data)
	dirID := hex.EncodeToString(sum[:])

	return dirID, data, nil
}

func CalculateFileID(blocks []Block) (string, []byte, error) {
	data, err := json.Marshal(blocks)
	if err != nil {
		return "", nil, err
	}

	sum := sha256.Sum256(data)
	fileID := hex.EncodeToString(sum[:])


	return fileID, data, nil
}
