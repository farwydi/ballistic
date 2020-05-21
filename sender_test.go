package ballistic

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

func TestSender(t *testing.T) {
	suite.Run(t, new(senderTestSuite))
}

type senderTestSuite struct {
	suite.Suite
}

func (suite *senderTestSuite) TestTwo() {
	db, sm, err := sqlmock.New()
	require.NoError(suite.T(), err, "An error was not expected when opening a stub database connection")
	sm.ExpectBegin()
	stmt := sm.ExpectPrepare("SELECT ?").
		WillBeClosed()
	stmt.ExpectExec().
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 0))
	stmt.ExpectExec().
		WithArgs(2).
		WillReturnResult(sqlmock.NewResult(0, 0))
	sm.ExpectCommit()
	s := NewSender(NewNullDumper(), db)
	s.SubscribeOnFail(func(err error) {
		assert.NoError(suite.T(), err, "Fail send")
	})
	s.Push("SELECT $1", 1)
	s.Push("SELECT $1", 2)
	s.RunPusher(time.Millisecond)
	time.Sleep(2 * time.Millisecond)
}

func (suite *senderTestSuite) TestOnePush() {
	db, sm, err := sqlmock.New()
	require.NoError(suite.T(), err, "An error was not expected when opening a stub database connection")
	sm.ExpectBegin()
	stmt := sm.ExpectPrepare("SELECT ?").
		WillBeClosed()
	stmt.ExpectExec().
		WithArgs(1).
		WillReturnResult(sqlmock.NewResult(0, 0))
	sm.ExpectCommit()
	s := NewSender(NewNullDumper(), db)
	s.SubscribeOnFail(func(err error) {
		assert.NoError(suite.T(), err, "Fail send")
	})
	s.Push("SELECT $1", 1)
	s.RunPusher(time.Millisecond)
	time.Sleep(2 * time.Millisecond)
}
