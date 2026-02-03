package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"

	"github.com/jotfs/fastcdc-go"
)

func ChunkFile(filePath string, db BlockStore) ([]Block, error) {
	// 1. Open the file
	f, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	// 2. Initialize the FastCDC Chunker
	// Default options: min=16KB, avg=64KB, max=256KB
	opts := fastcdc.Options{
		MinSize:     256 * 1024,   
		AverageSize: 1 * 1024 * 1024,  
		MaxSize:     4 * 1024 * 1024,    
	}
	chnkr, err := fastcdc.NewChunker(f, opts)
	if err != nil {
		return nil, err
	}

	var blocks []Block
	hasher := sha256.New()

	for {
		chunk, err := chnkr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		hasher.Reset()
		hasher.Write(chunk.Data)
		hashString := hex.EncodeToString(hasher.Sum(nil))

		// Save raw data to the database
		// If the block already exists, db ignores
		if err := db.SaveBlock(hashString, chunk.Data); err != nil {
			return nil, err
		}

		// 5. Create our Block struct
		b := Block{
			ID:   hashString,
			Size: int64(chunk.Length),
		}
		blocks = append(blocks, b)

		// fmt.Printf("Found Chunk: ID=%s... Size=%d bytes\n", hashString[:8], b.Size)
	}

	return blocks, nil
}