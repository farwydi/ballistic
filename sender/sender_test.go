package sender

import (
	"context"
	"testing"
)

func TestAppendMap(t *testing.T) {
	m := map[string][][]interface{}{}

	m["test"] = append(m["test"], nil)
	m["test"] = append(m["test"], nil)
}

func TestSenderStopByCtx(t *testing.T) {
	ctx, off := context.WithCancel(context.Background())

	s := NewSender(nil)

	go s.RunPusher(ctx)
	off()
	s.Stop(false)
}
