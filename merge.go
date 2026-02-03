package main

import (
	"path/filepath"
)

type Change struct {
	Path   string
	Action string // "ADDED", "REMOVED", "MODIFIED"
}


// Compare using a zipper
// Directory entries are sorted by name
func diff(db MetaStore, oldrootID, newrootID string, path string) ([]Change, error) {
	if oldrootID == newrootID {
		return nil, nil
	}	

	oldEntries, err := db.GetDirEntries(oldrootID)
	if err != nil { return nil, err }
	newEntries, err := db.GetDirEntries(newrootID)
	if err != nil { return nil, err }

	var i, j int
	var changes []Change

	for i < len(oldEntries) && j < len(newEntries) {
		oldEntry := oldEntries[i] // DirEntry
		newEntry := newEntries[j]

		if oldEntry.Name < newEntry.Name { // oldEntry is removed
			subChanges, err := explode(db, oldEntry, path, "REMOVED")
			if err != nil { return nil, err }
			changes = append(changes, subChanges...)
			i++
		} else if oldEntry.Name > newEntry.Name {
			subChanges, err := explode(db, newEntry, path, "ADDED")
			if err != nil { return nil, err }
			changes = append(changes, subChanges...)
			j++
		} else { // same name
			if oldEntry.Type != newEntry.Type {
				subChanges, err := explode(db, oldEntry, path, "REMOVED")
				if err != nil { return nil, err }
				changes = append(changes, subChanges...)
				subChanges, err = explode(db, newEntry, path, "ADDED")
				if err != nil { return nil, err }
				changes = append(changes, subChanges...)

			} else { // types and names match
				if oldEntry.ID != newEntry.ID {
					if oldEntry.Type == "file" {
						changes = append(changes, Change{
							Path: filepath.Join(path, oldEntry.Name),
							Action: "MODIFIED",
						})
					} else if oldEntry.Type == "dir" {
						subChanges, err := diff(db, oldEntry.ID, newEntry.ID, filepath.Join(path, oldEntry.Name))
						if err != nil { return nil, err }
						changes = append(changes, subChanges...)
					}
				}
			}
			i++
			j++
		} // end of same name
	}

	for i < len(oldEntries) {
		subChanges, err := explode(db, oldEntries[i], path, "REMOVED")
		if err != nil { return nil, err }
		changes = append(changes, subChanges...)
		i++
	}

	for j < len(newEntries) {
		subChanges, err := explode(db, newEntries[j], path, "ADDED")
		if err != nil { return nil, err }
		changes = append(changes, subChanges...)
		j++
	}

	return changes, nil
}


func explode(db MetaStore, entry DirEntry, path, action string) ([]Change, error) {
	var changes []Change
	fullPath := filepath.Join(path, entry.Name)

	if entry.Type == "file" {
		changes = append(changes, Change{
			Path:   fullPath,
			Action: action,
		})
	} else if entry.Type == "dir" {
		changes = append(changes, Change{
			Path:   fullPath,
			Action: action,
		})
		children, err := db.GetDirEntries(entry.ID)
		if err != nil { return nil, err }

		for _, child := range children {
			subChanges, err := explode(db, child, fullPath, action)
			if err != nil { return nil, err }
			changes = append(changes, subChanges...)
		}
	}
	return changes, nil
}
