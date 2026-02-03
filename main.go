package main

import (
	"fmt"
	"time"
)

func main() {
    rootPath := "./testdir"

    db, err := NewSQLStore("metadata.db")
    if err != nil { panic(err) }

    blockStore, err := NewDiskBlockStore("blocks")
    if err != nil { panic(err) }

    store := CompositeStore{
        BlockStore: blockStore,
        MetaStore: db,
        CommitStore: db,
    }

    rootID, err := ProcessDirectory(rootPath, store)
    if err != nil { panic(err) }

    fmt.Println("Processing Complete")
    fmt.Printf("Root Hash: %s\n", rootID)

    newCommitID, err := store.Commit("", rootID, time.Now().Unix())
    if err != nil { panic(err) }
    fmt.Printf("Commit Head (Hash): %s\n", newCommitID)

	err = build(store, "./output", rootID)
	if err != nil { panic(err) }

    fmt.Println("Build complete")
}

