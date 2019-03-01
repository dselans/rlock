package rlock

import (
	"errors"
	"fmt"
	"time"
	"database/sql"

	golog "github.com/InVisionApp/go-logger"
	gologShim "github.com/InVisionApp/go-logger/shims/logrus"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/types"
	"github.com/satori/go.uuid"

)

var (
	AcquireTimeoutErr = errors.New("reached timeout while waiting on lock")
	KeyNotFoundErr = errors.New("no such lock")

	log golog.Logger
)

func init() {
	log = gologShim.New(nil).WithFields(golog.Fields{"pkg": "rlock"})
}

const (
	TableName    = "rlock"
	PollInterval = 1 * time.Second
	MaxAge       = 1 * time.Hour
)

type IRLock interface {
	Lock(name string, acquireTimeout time.Duration) (*Lock, error)
}

type RLock struct {
	db    *sqlx.DB
	owner string
}

type Lock struct {
	rl      *RLock
	name    string
	timeout time.Duration
}

type LockEntry struct {
	ID        int           `db:"id"`
	Name      string        `db:"name"`
	Owner     string        `db:"owner"`
	InUse     types.BitBool `db:"in_use"`
	LastError string        `db:"last_error"`
	LastUsed  time.Time     `db:"last_used"`
	CreatedAt time.Time     `db:"created_at"`
}

func New(db *sqlx.DB) (*RLock, error) {
	if db == nil {
		return nil, fmt.Errorf("db cannot be nil")
	}

	return &RLock{
		db:    db,
		owner: generateUUID().String(),
	}, nil
}

func (r *RLock) Lock(name string, acquireTimeout time.Duration) (*Lock, error) {
	// try to insert a lock
	// if success -> return lock
	//
	// if failure -> check the existing lock
	//	is existing lock valid (ie. is the existing lock NOT stale?)
	//	poll until it gets released OR disappears OR we timeout
	query := fmt.Sprintf("INSERT INTO %v (name, owner, in_use) VALUES(?, ?, 1)", TableName)

	dupe := false

	if _, err := r.db.Exec(query, name, r.owner); err != nil {
		// Is this a dupe? MySQL error codes documented here: https://dev.mysql.com/doc/refman/5.7/en/error-messages-server.html
		if me, ok := err.(*mysql.MySQLError); ok && me.Number == 1062 {
			dupe = true
		} else {
			return nil, fmt.Errorf("unable to insert lock for '%s': %v", name, err)
		}
	}

	// No error, no dupe
	if !dupe {
		return &Lock{
			rl:      r,
			name:    name,
			timeout: acquireTimeout,
		}, nil
	}

	// Got an error, but it was a dupe, let's inspect the lock
	existingLock, err := r.getExistingByName(name)
	if err != nil {
		if err == KeyNotFoundErr {
			return nil, fmt.Errorf("lock no longer exists")
		}

		return nil, fmt.Errorf("unable to fetch existing lock: %v", err)
	}

	// If the existing lock is invalid, take it over
	if err := isValid(existingLock, name, acquireTimeout); err != nil {
		// Existing lock is not valid
		if err := r.takeover(name, existingLock.Owner, true); err != nil {
			return nil, fmt.Errorf("unable to take over lock '%v': %v", name, err)
		}

		return &Lock{
			rl:      r,
			name:    name,
			timeout: acquireTimeout,
		}, nil
	}

	// Existing lock is valid, poll and block until it becomes available OR
	// we hit acquireTimeout
	timer := time.NewTimer(acquireTimeout)

	for {
		select {
		case <-timer.C:
			return nil, AcquireTimeoutErr
		default:
			time.Sleep(PollInterval)
			if err := r.takeover(name, existingLock.Owner, false); err != nil {
				continue
			}

			// We acquired a lock!
			return &Lock{
				rl:      r,
				name:    name,
				timeout: acquireTimeout,
			}, nil
		}
	}
}

// Try to take over an existing lock; if force is false, we will only take over
// the lock when in_use is false; if force is true, we will take over
// the lock, regardless of state of in_use.
func (r *RLock) takeover(origName, origOwner string, force bool) error {
	query := fmt.Sprintf("UPDATE %v SET owner=?, in_use=1 WHERE name=? AND in_use=0 AND owner=?", TableName)

	if force {
		query = fmt.Sprintf("UPDATE %v SET owner=?, in_use=1 WHERE name=? AND owner=?", TableName)
	}

	res, err := r.db.Exec(query, r.owner, origName, origOwner)
	if err != nil {
		return fmt.Errorf("unable to take over '%v': %v", origName, err)
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("unable to determine rows affected during takeover for '%v': %v", origName, err)
	}

	if affected > 1 {
		return fmt.Errorf("lock takeover affected more than 1 row, possible bug")
	}

	if affected == 0 {
		return fmt.Errorf("unable to takeover lock, still in use")
	}

	// Lock takeover succeeded
	return nil
}

func (r *RLock) getExistingByName(name string) (*LockEntry, error) {
	query := fmt.Sprintf("SELECT * FROM %v WHERE name=?", TableName)

	entry := &LockEntry{}

	if err := r.db.Get(entry, query, name); err != nil {
		if err == sql.ErrNoRows {
			return nil, KeyNotFoundErr
		}

		return nil, err
	}

	return entry, nil
}

// Verify that the existing lock is in good condition (and should be trusted).
//
// ie. is it stale?
func isValid(existingLock *LockEntry, newLockName string, newLockTimeout time.Duration) error {
	if existingLock == nil {
		return fmt.Errorf("existing lock cannot be nil")
	}

	if !existingLock.InUse {
		return fmt.Errorf("existing lock is not in use")
	}

	if time.Since(existingLock.LastUsed) > MaxAge {
		return fmt.Errorf("existing lock is stale")
	}

	return nil
}

// If an error is passed to unlock, upon unlocking the row in the db, we will
// also update `last_used` to the passed error. This way, subsequent lock
// holders can call on LastError() and see what (if any) error previous
// lock holder(s) ran into.
func (l *Lock) Unlock(lastError error) error {
	query := fmt.Sprintf("UPDATE %v SET in_use=0, last_error=? WHERE name=? AND owner=?", TableName)

	var lastErrorStr string

	if lastError == nil {
		lastErrorStr = ""
	} else {
		lastErrorStr = lastError.Error()
	}

	result, err := l.rl.db.Exec(query, lastErrorStr, l.name, l.rl.owner)
	if err != nil {
		fullErr := fmt.Errorf("unable to unlock '%v': %v", l.name, err)
		log.Error(fullErr)
		return fullErr
	}

	affected, err := result.RowsAffected()
	if err != nil {
		fullErr := fmt.Errorf("unable to determine affected rows after unlock for '%v': %v", l.name, err)
		log.Error(fullErr)
		return fullErr
	}

	if affected != 1 {
		fullErr := fmt.Errorf("unexpected number of affected rows after unlock (%d)", affected)
		log.Error(fullErr)
		return fullErr
	}

	// Unlocked successfully
	return nil
}

// LastError returns nil if `last_used` is empty or an error if `last_used` is
// not empty.
//
// `last_error` (in the db) is used by *current* lock holders to convey state
// such as whether they ran into an error. A subsequent lock holder can then
// determine whether the previous lock user ran into issues AND potentially
// perform additional steps based on the answer.
func (l *Lock) LastError() error {
	query := fmt.Sprintf("SELECT last_error FROM %v WHERE name=? AND owner=?", TableName)

	var lastError string
	if err := l.rl.db.Get(&lastError, query, l.name, l.rl.owner); err != nil {
		return fmt.Errorf("unexpected error while fetching last error state: %v", err)
	}

	if lastError == "" {
		return nil
	}

	return errors.New(lastError)
}

// Namespace uuid was generated via `uuidgen`
var nsUUID = uuid.Must(uuid.FromString("3cd4853f-ad8f-40f9-8558-014dd707b7b4"))

// GenerateUUID by iterating over the strategies, without throwing an error
func generateUUID() uuid.UUID {
	id, err := uuid.NewV1()
	if err != nil {
		id, err = uuid.NewV4()
		if err != nil {
			return uuid.NewV5(nsUUID, time.Now().String())
		}
	}

	return id
}
