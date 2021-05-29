package file

import (
	"errors"
	"fmt"
	"github.com/farwydi/ballistic"
	"hash/adler32"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
)

func NewQueueByModel(model ballistic.DataModel, config ...Config) (*Queue, error) {
	// Set default config
	cfg := configDefault(config...)

	return (&queueLoader{
		cfg:               cfg,
		fileNameExtractor: regexp.MustCompile(`^(\d+)_(\d+)\.(bd|carapted)$`),
	}).load(model)
}

type queueLoader struct {
	cfg               Config
	fileNameExtractor *regexp.Regexp
}

func (q *queueLoader) load(model ballistic.DataModel) (*Queue, error) {
	h := adler32.New()
	_, _ = h.Write([]byte(model.SQL()))

	fName := fmt.Sprintf("%d_0.bd", h.Sum32())
	fPath := filepath.Join(q.cfg.Workspace, fName)
	file, err := os.OpenFile(fPath, os.O_CREATE|os.O_RDWR, os.ModePerm)
	if err != nil {
		return nil, err
	}

	queue, err := NewQueue(file, model)
	if err != nil {
		if errors.Is(err, ErrInvalidFile) {
			err := q.markCarapted(file)
			if err != nil {
				return nil, err
			}

			file, err = os.OpenFile(fPath, os.O_CREATE|os.O_RDWR, os.ModePerm)
			queue, err = NewQueue(file, model)
			if err != nil {
				return nil, err
			}
		}
		return nil, err
	}
	return queue, nil
}

func (q *queueLoader) markCarapted(file *os.File) error {
	err := file.Close()
	if err != nil {
		return err
	}

	name, _, n, err := q.extractName(filepath.Base(file.Name()))
	if err != nil {
		return err
	}
	caraptedFilePath := filepath.Join(q.cfg.Workspace, q.buildName(name, "carapted", n))

	return q.move(file.Name(), caraptedFilePath)
}

func (q *queueLoader) buildName(name, t string, n int) string {
	return fmt.Sprintf("%s_%d.%s", name, n, t)
}

func (q *queueLoader) extractName(fileName string) (name, t string, n int, err error) {
	fne := q.fileNameExtractor.FindStringSubmatch(fileName)
	if len(fne) != 4 {
		return "", "", 0, fmt.Errorf("bad name: '%s'", fileName)
	}

	n, err = strconv.Atoi(fne[2])
	if err != nil {
		return "", "", 0, err
	}

	return fne[1], fne[3], n, nil
}

func (q *queueLoader) move(prev, next string) error {
	if exists(next) {
		name, t, n, err := q.extractName(filepath.Base(next))
		if err != nil {
			return err
		}

		err = q.move(next, filepath.Join(q.cfg.Workspace, q.buildName(name, t, n+1)))
		if err != nil {
			return err
		}
	}

	_, _, n, err := q.extractName(filepath.Base(prev))
	if err != nil {
		return err
	}

	if n >= q.cfg.MaxHistory {
		return os.Remove(prev)
	}

	return os.Rename(prev, next)
}
