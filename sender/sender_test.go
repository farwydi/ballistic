package sender

import (
	"context"
	"testing"
	"time"
)

func TestAppendMap(t *testing.T) {
	m := map[string][][]interface{}{}

	m["test"] = append(m["test"], nil)
	m["test"] = append(m["test"], nil)
}

func TestSenderStopByCtx(t *testing.T) {
	ctx, off := context.WithCancel(context.Background())

	s := NewSender(nil, Config{SendInterval: 100 * time.Millisecond})

	go s.RunPusher(ctx)
	off()

	<-time.After(101 * time.Millisecond)

	s.Stop(false)
}
