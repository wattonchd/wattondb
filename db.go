package wattondb

import (
	"io"
	"os"
	"sync"
)

type MiniDB struct {
	indexes map[string]int64 // 内存中的索引信息
	dbFile  *DBFile          // 数据文件
	dirPath string           // 数据目录
	mu      sync.Mutex
}

// loadIndexesFromFile 从文件中记载索引
func (db *MiniDB) loadIndexesFromFile() {
	if db.dbFile == nil {
		return
	}

	var offset int64
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return
		}
		// 设置索引状态
		db.indexes[string(e.Key)] = offset

		if e.Mark == DEL {
			delete(db.indexes, string(e.Key))
		}
		offset += e.GetSize()
	}
	return
}

func Open(dirPath string) (*MiniDB, error) {
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(dirPath, os.ModePerm); err != nil {
			return nil, err
		}
	}

	dbFile, err := NewDBFile(dirPath)
	if err != nil {
		return nil, err
	}

	db := &MiniDB{
		indexes: make(map[string]int64),
		dbFile:  dbFile,
		dirPath: dirPath,
	}
	// 从文件中加载索引
	db.loadIndexesFromFile()
	return nil, err
}

// todo put get del

// Merge 合并数据文件
func (db *MiniDB) Merge() error {
	// 没有数据则忽略
	if db.dbFile.Offset == 0 {
		return nil
	}
	var (
		validEntries []*Entry
		offset       int64
	)
	// 读取原数据文件中的 Entry
	for {
		e, err := db.dbFile.Read(offset)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		// 内存中的索引状态是最新的，直接对比过滤出有效的 Entry
		if off, ok := db.indexes[string(e.Key)]; ok && off == offset {
			validEntries = append(validEntries, e)
		}
		offset += e.GetSize()
	}

	if len(validEntries) > 0 {
		mergeDBfile, err := NewMergeDBFile(db.dirPath)
		if err != nil {
			return err
		}
		defer os.Remove(mergeDBfile.File.Name())

		// 重新写入有效的Entry
		for _, entry := range validEntries {
			writeOff := mergeDBfile.Offset
			err := mergeDBfile.Write(entry)
			if err != nil {
				return err
			}

			// 更新索引
			db.indexes[string(entry.Key)] = writeOff
		}
		// 获取文件名
		dbFileName := db.dbFile.File.Name()
		// 关闭文件
		db.dbFile.File.Close()
		// 删除旧的数据文件
		os.Remove(dbFileName)

		// 获取文件名
		mergeDBFileName := mergeDBfile.File.Name()
		// 关闭文件
		mergeDBfile.File.Close()
		// 临时文件变更为新的数据文件
		os.Rename(mergeDBFileName, db.dirPath+string(os.PathSeparator)+FileName)

		db.dbFile = mergeDBfile
	}
	return nil
}

// Put 写入数据
func (db *MiniDB) Put(key, value []byte) (err error) {
	return
}

// Get 取出数据
func (db *MiniDB) Get(key []byte) (val []byte, err error) {
	return
}

// Del 删除数据
func (db *MiniDB) Del(key []byte) (err error) {
	return
}
