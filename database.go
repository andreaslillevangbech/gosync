package main


import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"
)

type Store interface {
	BlockStore
	MetaStore
	CommitStore
}

type BlockStore interface {
	SaveBlock(id string, data []byte) error
	GetBlock(id string) ([]byte, error)
	HasBlock(id string) (bool, error)
}

type MetaStore interface {
	SaveMetadata(id, typeStr string, data []byte) error
	GetDirEntries(id string) ([]DirEntry, error)
	GetFileBlocks(id string) ([]Block, error)
}

type CommitStore interface {
	// Commit the latest root ID
	Commit(parentid, rootid string, timestamp int64) (string, error)
	GetCommit(id string) (*Commit, error)
	GetLatestCommit() (string, error)

}


type Commit struct {
	ParentID  string `json:"parent_id"` // Empty if first commit
	RootID    string `json:"root_id"`   // The hash returned by ProcessDirectory
	Timestamp int64  `json:"timestamp"`
}


// SQLStore implements your Storage interface
type SQLStore struct {
	db *sql.DB
}

func NewSQLStore(dbPath string) (*SQLStore, error) {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil { return nil, err }
	
    // Create tables if they don't exist
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS blocks (id TEXT PRIMARY KEY, data BLOB);
		CREATE TABLE IF NOT EXISTS metadata (id TEXT, obj_type TEXT, data BLOB, PRIMARY KEY (id, obj_type));
		CREATE TABLE IF NOT EXISTS commits (id TEXT PRIMARY KEY, parent_id TEXT, root_id TEXT, timestamp INTEGER);
	`)
	return &SQLStore{db: db}, err
}

// Commit -> Goes to Table 3 (commits)
func (s *SQLStore) Commit(parentid string, rootid string, timestamp int64) (string, error) {
	commit := Commit{
		ParentID: parentid,
		RootID: rootid,
		Timestamp: timestamp,
	}

	jsonCommit, err := json.Marshal(commit)
	if err != nil { return "", err }

	// Hash the commit
	sum := sha256.Sum256(jsonCommit)
	id := hex.EncodeToString(sum[:])

	query := `INSERT OR IGNORE INTO commits (id, parent_id, root_id, timestamp) VALUES (?, ?, ?, ?)`
	_, err = s.db.Exec(query, id, parentid, rootid, timestamp)
	return id, err
}

func (s *SQLStore) GetCommit(id string) (*Commit, error) {
	query := `SELECT parent_id, root_id, timestamp FROM commits WHERE id = ?`
	var commit Commit
	err := s.db.QueryRow(query, id).Scan(&commit.ParentID, &commit.RootID, &commit.Timestamp)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("commit not found for id: %s", id)
	}
	return &commit, err
}
func (s *SQLStore) GetLatestCommit() (string, error) {
	query := `SELECT id FROM commits ORDER BY timestamp DESC LIMIT 1`
	var id string
	err := s.db.QueryRow(query).Scan(&id)
	if err == sql.ErrNoRows {
		return "", fmt.Errorf("no commits found")
	}
	commit, err := s.GetCommit(id)
	if err != nil { return "", err }
	return commit.RootID, nil
}

// SaveBlock -> Goes to Table 1
func (s *SQLStore) SaveBlock(id string, data []byte) error {
	// "INSERT OR IGNORE" handles deduplication automatically!
	query := `INSERT OR IGNORE INTO blocks (id, data) VALUES (?, ?)`
	
    _, err := s.db.Exec(query, id, data)
	if err != nil { return err }
	
	return nil
}

// SaveMetadata -> Goes to Table 2
func (s *SQLStore) SaveMetadata(id string, objType string, data []byte) error {
	// "INSERT OR IGNORE" works here too.
    // If we've already saved this exact folder structure, we don't need to do it again.
	query := `INSERT OR IGNORE INTO metadata (id, obj_type, data) VALUES (?, ?, ?)`
	
    _, err := s.db.Exec(query, id, objType, data)
	if err != nil { return err }
	
	return nil
}

func (s *SQLStore) GetDirEntries(id string) ([]DirEntry, error) {
	query := `SELECT data FROM metadata WHERE id = ? AND obj_type = 'dir'`
	var data []byte
	err := s.db.QueryRow(query, id).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("directory not found for id: %s", id)
	}
	if err != nil { return nil, err }

	var entries []DirEntry
	if err := json.Unmarshal(data, &entries); err != nil {
		return nil, fmt.Errorf("failed to unmarshal directory entries: %w", err)
	}
	return entries, nil
}

func (s *SQLStore) GetFileBlocks(id string) ([]Block, error) {
	query := `SELECT data FROM metadata WHERE id = ? AND obj_type = 'file'`
	var data []byte
	err := s.db.QueryRow(query, id).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("file not found for id: %s", id)
	}
	if err != nil { return nil, err }

	var blocks []Block
	if err := json.Unmarshal(data, &blocks); err != nil {
		return nil, fmt.Errorf("failed to unmarshal file blocks: %w", err)
	}
	return blocks, nil
}


func (s *SQLStore) GetBlock(id string) ([]byte, error) {
	query := `SELECT data FROM blocks WHERE id = ?`
	var data []byte
	err := s.db.QueryRow(query, id).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("block not found for id: %s", id)
	}
	if err != nil { return nil, err }
	return data, nil
}

func (s *SQLStore) HasBlock(id string) (bool, error) {
	query := `SELECT COUNT(*) FROM blocks WHERE id = ?`
	var count int
	err := s.db.QueryRow(query, id).Scan(&count)
	if err != nil { return false, err }
	return count > 0, nil
}

// DiskBlockStore stores blocks as files on disk
type DiskBlockStore struct {
	basePath string
}

func NewDiskBlockStore(path string) (*DiskBlockStore, error) {
	if err := os.MkdirAll(path, 0755); err != nil {
		return nil, err
	}
	return &DiskBlockStore{basePath: path}, nil
}

func (f *DiskBlockStore) SaveBlock(id string, data []byte) error {
	if len(id) < 2 {
        return fmt.Errorf("invalid id: too short")
    }

	dirPath := filepath.Join(f.basePath, id[:2])
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return err
	}
	path := filepath.Join(dirPath, id[2:])

	// Deduplication
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	return os.WriteFile(path, data, 0644)
}

func (f *DiskBlockStore) GetBlock(id string) ([]byte, error) {
	if len(id) < 2 {
        return nil, fmt.Errorf("invalid id: too short")
    }
	data, err := os.ReadFile(filepath.Join(f.basePath, id[:2], id[2:]))
	if os.IsNotExist(err) {
		return nil, fmt.Errorf("block not found for id: %s", id)
	}
	return data, err
}

func (f *DiskBlockStore) HasBlock(id string) (bool, error) {
	_, err := os.Stat(filepath.Join(f.basePath, id[:2], id[2:]))
	if os.IsNotExist(err) {
		return false, nil
	}
	return err == nil, err
}

// CompositeStore combines different store implementations
type CompositeStore struct {
	BlockStore
	MetaStore
	CommitStore
}

