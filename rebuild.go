package main

import (
	"path/filepath"
	"os"
)


func build(db Store, path, id string) error {
	entries, err := db.GetDirEntries(id)
	if err != nil { return err }

	// make dir	
	os.MkdirAll(path, 0755)

	for _, entry := range entries {
		if entry.Type == "file" {
			err = buildFile(db, filepath.Join(path, entry.Name), entry.ID)
			if err != nil { return err }
		} else {
			err = build(db, filepath.Join(path, entry.Name), entry.ID)
			if err != nil { return err }
		}
	}
	return nil
}


func buildFile(db Store, path, fileid string) error {
	

	blocks, err := db.GetFileBlocks(fileid)
	if err != nil { return err }

	var fileData []byte
	for _, block := range blocks {
		blockData, err := db.GetBlock(block.ID)
		if err != nil { return err }
		fileData = append(fileData, blockData...)
	}

	// Write file to disk
	os.WriteFile(path, fileData, 0644)

	return nil
}
