package wattondb

import "os"

const FileName = "minidb.data"
const MergeFileName = "minidb.data.merge"

type DBFile struct {
	File   *os.File
	Offset int64
}

func NewInternal(filename string) (*DBFile, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}

	stat, err := os.Stat(filename)
	if err != nil {
		return nil, err
	}
	return &DBFile{File: file, Offset: stat.Size()}, nil
}

// NewDBFile 创建一个新的数据文件
func NewDBFile(path string) (*DBFile, error) {
	filename := path + string(os.PathSeparator) + FileName
	return NewInternal(filename)
}

// NewMergeDBFile 创建一个合并时的数据文件
func NewMergeDBFile(path string) (*DBFile, error) {
	filename := path + string(os.PathSeparator) + MergeFileName
	return NewInternal(filename)
}

// Read 从 offset 处开始读取
func (df *DBFile) Read(offset int64) (e *Entry, err error) {
	buf := make([]byte, entryHeadSize)
	if _, err = df.File.ReadAt(buf, offset); err != nil {
		return
	}
	/*
		相当于:
		e, err = Decode(buf)
		if err != nil {
			return
		}
	*/
	if e, err = Decode(buf); err != nil {
		return
	}
	offset += entryHeadSize
	if e.KeySize > 0 {
		key := make([]byte, e.KeySize)
		if _, err = df.File.ReadAt(key, offset); err != nil {
			return
		}
		e.Key = key
	}

	offset += int64(e.KeySize)
	if e.ValueSize > 0 {
		value := make([]byte, e.ValueSize)
		if _, err = df.File.ReadAt(value, offset); err != nil {
			return
		}
		e.Value = value
	}
	return
}

// Write 写入Entry
func (df *DBFile) Write(e *Entry) (err error) {
	enc, err := e.Encode()
	if err != nil {
		return err
	}
	_, err = df.File.WriteAt(enc, df.Offset)
	df.Offset += e.GetSize()
	return
}
