package file

import (
	"encoding"
	"encoding/binary"
	"errors"
	"fmt"
	"hash"
	"hash/crc32"
	"io"
	"math"
	"os"
	"reflect"
	"sync"
)

type Safe interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
}

const (
	CRC32HashOffset int64 = 0
	CRC32HashSize   int64 = 4
	SkipAheadOffset       = CRC32HashOffset + CRC32HashSize
	SkipAheadSize   int64 = 8
	DataOffset            = SkipAheadOffset + SkipAheadSize
	HeadSize              = CRC32HashSize + SkipAheadSize
	MetaElementSize       = 2
)

func NewQueue(file *os.File, pattern Safe) (*Queue, error) {
	return (&Queue{
		typeOf: reflect.ValueOf(pattern).Elem().Type(),
		file:   file,
		order:  binary.BigEndian,
		sum:    crc32.NewIEEE(),
	}).checkFile()
}

type Queue struct {
	typeOf reflect.Type
	file   *os.File
	order  binary.ByteOrder
	mx     sync.Mutex

	sum   hash.Hash32
	count int
	mw    io.Writer
}

func (f *Queue) Len() int {
	f.mx.Lock()
	defer f.mx.Unlock()
	return f.count
}

func (f *Queue) checkFile() (*Queue, error) {
	f.mw = io.MultiWriter(f.file, f.sum)

	_, err := f.file.Seek(0, io.SeekStart)
	if err != nil {
		return nil, err
	}

	buf := make([]byte, HeadSize)
	crc32SumBuf := buf[0:CRC32HashSize]
	skipAheadBuf := buf[CRC32HashSize : CRC32HashSize+SkipAheadSize]

	n, err := f.file.Read(buf[0:HeadSize])
	if err != nil {
		if errors.Is(err, io.EOF) && n == 0 {
			haedBuf := buf[0 : CRC32HashSize+SkipAheadSize]
			f.order.PutUint32(crc32SumBuf, 0)
			f.order.PutUint64(skipAheadBuf, uint64(DataOffset))
			_, err := f.file.Write(haedBuf)
			if err != nil {
				return nil, err
			}
			return f, nil
		}
		return nil, err
	}

	fileSum := f.order.Uint32(crc32SumBuf)
	skipAhead := int64(f.order.Uint64(skipAheadBuf))
	currOffset := DataOffset

	_, err = f.file.Seek(DataOffset, io.SeekStart)
	if err != nil {
		return nil, err
	}

	tr := io.TeeReader(f.file, f.sum)

	for {
		size, err := f.readMeta(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		currOffset += MetaElementSize

		if size > math.MaxUint16 {
			return nil, ErrInvalidFile
		}

		if len(buf) < size {
			buf = make([]byte, size)
		}

		_, err = tr.Read(buf[:size])
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil, ErrInvalidFile
			}
			return nil, err
		}

		currOffset += int64(size)

		if currOffset > skipAhead {
			f.count++
		}
	}

	if f.sum.Sum32() != fileSum {
		return nil, ErrInvalidFile
	}

	return f, nil
}

func (f *Queue) readMeta(bs []byte) (size int, err error) {
	metaElementBuf := bs[0:MetaElementSize]

	_, err = f.file.Read(metaElementBuf)
	if err != nil {
		return 0, err
	}

	size = int(f.order.Uint16(metaElementBuf))

	return size, err
}

func (f *Queue) writeMeta(bs []byte, size int) error {
	metaElementBuf := bs[0:MetaElementSize]

	f.order.PutUint16(metaElementBuf, uint16(size))

	_, err := f.file.Write(metaElementBuf)
	if err != nil {
		return err
	}

	return nil
}

func (f *Queue) updateSum(bs []byte) error {
	crc32SumBuf := bs[0:CRC32HashSize]

	f.order.PutUint32(crc32SumBuf, f.sum.Sum32())
	_, err := f.file.WriteAt(crc32SumBuf, CRC32HashOffset)
	if err != nil {
		return err
	}

	return nil
}

func (f *Queue) Push(model encoding.BinaryMarshaler) error {
	data, err := model.MarshalBinary()
	if err != nil {
		return err
	}

	size := len(data)

	if size > math.MaxUint16 {
		return fmt.Errorf("model to large: %d over %d", size, math.MaxUint16)
	}

	bs := bsPool.Get().([]byte)
	defer bsPool.Put(bs)

	f.mx.Lock()
	defer f.mx.Unlock()

	err = f.writeMeta(bs, size)
	if err != nil {
		return err
	}

	_, err = f.mw.Write(data)
	if err != nil {
		return err
	}

	f.count++

	err = f.updateSum(bs)
	if err != nil {
		return err
	}

	return nil
}

func (f *Queue) Eject(limit int) (models []interface{}, err error) {
	f.mx.Lock()
	defer f.mx.Unlock()

	if limit > f.count {
		limit = f.count
	}

	if limit < 0 {
		limit = f.count
	}

	if limit == 0 {
		return nil, nil
	}

	models = make([]interface{}, limit)

	buf := make([]byte, HeadSize)
	skipAheadBuf := buf[0:SkipAheadSize]

	_, err = f.file.ReadAt(skipAheadBuf, SkipAheadOffset)
	if err != nil {
		return nil, err
	}

	skipAhead := int64(f.order.Uint64(skipAheadBuf))

	_, err = f.file.Seek(skipAhead, io.SeekStart)
	if err != nil {
		return nil, err
	}

	for i := 0; i < limit; i++ {
		size, err := f.readMeta(buf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return models[:i], err
		}

		skipAhead += MetaElementSize

		if len(buf) < size {
			buf = make([]byte, size)
		}

		dataBuf := buf[0:size]
		_, err = f.file.Read(dataBuf)
		f.count--
		if err != nil {
			return models[:i], err
		}

		skipAhead += int64(size)

		e := reflect.New(f.typeOf).Interface().(encoding.BinaryUnmarshaler)
		err = e.UnmarshalBinary(dataBuf)
		if err != nil {
			return models[:i], err
		}

		models[i] = e
	}

	f.order.PutUint64(skipAheadBuf, uint64(skipAhead))
	_, err = f.file.WriteAt(skipAheadBuf, SkipAheadOffset)
	if err != nil {
		return models, err
	}

	_, err = f.file.Seek(0, io.SeekEnd)
	if err != nil {
		return models, err
	}

	return models, nil
}
