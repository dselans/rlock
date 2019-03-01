package rlock

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"gopkg.in/DATA-DOG/go-sqlmock.v1"
	"time"
)

func setupMocks() (*sqlx.DB, sqlmock.Sqlmock, *RLock) {
	mockDB, mock, err := sqlmock.New()
	Expect(err).ToNot(HaveOccurred())

	db := sqlx.NewDb(mockDB, "sqlmock")

	rl, err := New(db)
	Expect(err).ToNot(HaveOccurred())

	return db, mock, rl
}

var _ = Describe("RLock", func() {
	var (
		existingLockName  = "existing-test-lock"
		existingLockOwner = "existing-test-lock-owner"
		newLockName       = "new-test-lock"
		//newLockOwner      = "new-test-lock-owner"
		acquireTimeout = 15 * time.Minute
	)

	Describe("New", func() {
		var (
			db   *sqlx.DB
		)

		BeforeEach(func() {
			db, _, _ = setupMocks()
		})

		Context("happy path", func() {
			It("should return an rlock instance", func() {
				rl, err := New(db)

				Expect(err).ToNot(HaveOccurred())
				Expect(rl).ToNot(BeNil())
				Expect(rl.owner).ToNot(BeEmpty())
				Expect(rl.db).ToNot(BeNil())
			})
		})

		Context("with nil sqlx.db", func() {
			It("should error", func() {
				rl, err := New(nil)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("cannot be nil"))
				Expect(rl).To(BeNil())
			})
		})
	})

	Describe("Lock", func() {
		var (
			mock sqlmock.Sqlmock
			rl   *RLock
		)

		BeforeEach(func() {
			_, mock, rl = setupMocks()
		})

		Context("happy path: when inserting a brand new lock (no dupe)", func() {
			It("inserts a lock and returns lock instance", func() {
				mock.ExpectExec(
					fmt.Sprintf(`INSERT INTO %v`, TableName)).
					WithArgs(newLockName, rl.owner).
					WillReturnResult(sqlmock.NewResult(1, 1))

				l, err := rl.Lock(newLockName, acquireTimeout)

				Expect(err).ToNot(HaveOccurred())
				Expect(l).ToNot(BeNil())
				Expect(l.name).To(Equal(newLockName))
				Expect(l.timeout).To(Equal(acquireTimeout))
				Expect(mock.ExpectationsWereMet()).ToNot(HaveOccurred())
			})
		})

		Context("when inserting a lock but there's a dupe lock", func() {
			Context("when the dupe lock is valid", func() {
				Context("we go into polling mode", func() {
					Context("when acquiring/takeover times out", func() {
						It("we return an acquireTimeout error", func() {

						})
					})

					Context("when acquiring/takeover succeeds", func() {
						It("we return an instance of lock and no error", func() {

						})
					})
				})
			})

			Context("when the dupe lock is NOT valid", func() {
				Context("lock takeover occurs && when lock takeover succeeds", func() {
					It("we will return a new lock instance", func() {

					})
				})

				Context("lock takeover occurs && results in error", func() {
					It("we will return an error", func() {

					})
				})
			})
		})

		Context("when inserting a lock but get mysql error", func() {
			It("should return error", func() {
				mock.ExpectExec(
					fmt.Sprintf(`INSERT INTO %v`, TableName)).
					WithArgs(newLockName, rl.owner).
					WillReturnError(fmt.Errorf("some error"))

				l, err := rl.Lock(newLockName, acquireTimeout)

				Expect(err).To(HaveOccurred())
				Expect(l).To(BeNil())
				Expect(err.Error()).To(ContainSubstring("unable to insert lock for"))
				Expect(err.Error()).To(ContainSubstring("some error"))
			})
		})
	})

	Describe("takeover", func() {
		var (
			mock sqlmock.Sqlmock
			rl   *RLock
		)

		BeforeEach(func() {
			_, mock, rl = setupMocks()
		})

		Context("happy path: when given a valid existingLock", func() {
			Context("with force set to false", func() {
				It("lock takeover succeeds", func() {
					// ensure our query contains "WHERE in_use = 0"
					mock.ExpectExec(
						fmt.Sprintf(`^UPDATE %v SET owner=.+,\s+in_use=1 WHERE name=.+\s+AND\s+in_use=0 AND owner=.+$`, TableName)).
						WithArgs(rl.owner, existingLockName, existingLockOwner).
						WillReturnResult(sqlmock.NewResult(1, 1))

					err := rl.takeover(existingLockName, existingLockOwner, false)

					Expect(err).ToNot(HaveOccurred())
					Expect(mock.ExpectationsWereMet()).ToNot(HaveOccurred())
				})
			})

			Context("with force set to true", func() {
				It("lock takeover succeeds", func() {
					// ensure our query does NOT contain "WHERE in_use = 0"
					mock.ExpectExec(
						fmt.Sprintf(`^UPDATE %v SET owner=.+, in_use=1 WHERE name=.+\s+AND owner=.+$`, TableName)).
						WithArgs(rl.owner, existingLockName, existingLockOwner).
						WillReturnResult(sqlmock.NewResult(1, 1))

					err := rl.takeover(existingLockName, existingLockOwner, true)

					Expect(err).ToNot(HaveOccurred())
					Expect(mock.ExpectationsWereMet()).ToNot(HaveOccurred())

				})
			})
		})

		Context("when query execution fails", func() {
			It("returns an error", func() {
				mock.ExpectExec(
					fmt.Sprintf(`^UPDATE %v SET owner=.+,\s+in_use=1 WHERE name=.+\s+AND\s+in_use=0 AND owner=.+$`, TableName)).
					WillReturnError(fmt.Errorf("something broke"))

				err := rl.takeover(existingLockName, existingLockOwner, false)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("something broke"))
				Expect(mock.ExpectationsWereMet()).ToNot(HaveOccurred())
			})
		})

		Context("when RowsAffected fails", func() {
			It("returns an error", func() {
				mock.ExpectExec(
					fmt.Sprintf(`^UPDATE %v SET owner=.+,\s+in_use=1 WHERE name=.+\s+AND\s+in_use=0 AND owner=.+$`, TableName)).
					WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("affected broke")))

				err := rl.takeover(existingLockName, existingLockOwner, false)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unable to determine rows affected during takeover"))
				Expect(err.Error()).To(ContainSubstring("affected broke"))
			})
		})

		Context("when affected rows is larger than 1", func() {
			It("returns an error", func() {
				mock.ExpectExec(
					fmt.Sprintf(`^UPDATE %v SET owner=.+,\s+in_use=1 WHERE name=.+\s+AND\s+in_use=0 AND owner=.+$`, TableName)).
					WillReturnResult(sqlmock.NewResult(1, 2))

				err := rl.takeover(existingLockName, existingLockOwner, false)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("lock takeover affected more than 1 row, possible bug"))
			})
		})

		Context("when affected rows is 0", func() {
			It("returns an error", func() {
				mock.ExpectExec(
					fmt.Sprintf(`^UPDATE %v SET owner=.+,\s+in_use=1 WHERE name=.+\s+AND\s+in_use=0 AND owner=.+$`, TableName)).
					WillReturnResult(sqlmock.NewResult(1, 0))

				err := rl.takeover(existingLockName, existingLockOwner, false)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unable to takeover lock, still in use"))
			})
		})
	})

	Describe("getExistingByName", func() {
		var (
			mock sqlmock.Sqlmock
			rl   *RLock
		)

		BeforeEach(func() {
			_, mock, rl = setupMocks()
		})

		Context("when given a valid lock name", func() {
			It("returns a lock entry", func() {

				rows := sqlmock.NewRows([]string{
					"id", "name", "owner", "in_use", "last_error", "last_used", "created_at",
				}).AddRow(1, newLockName, "owner-name", []byte{1}, "", time.Now(), time.Now())

				mock.ExpectQuery(`SELECT \* FROM`).
					WithArgs(newLockName).
					WillReturnRows(rows)

				entry, err := rl.getExistingByName(newLockName)

				Expect(err).ToNot(HaveOccurred())
				Expect(entry).ToNot(BeNil())
				Expect(entry.Name).To(Equal(newLockName))
				Expect(entry.ID).To(Equal(1))
				Expect(entry.Owner).To(Equal("owner-name"))
				Expect(mock.ExpectationsWereMet()).ToNot(HaveOccurred())
				Expect(bool(entry.InUse)).To(BeTrue())
			})
		})

		Context("when db returns no results", func() {
			It("returns KeyNotFound error", func() {
				mock.ExpectQuery(`SELECT \* FROM`).
					WillReturnError(sql.ErrNoRows)

				entry, err := rl.getExistingByName(newLockName)

				Expect(err).To(HaveOccurred())
				Expect(entry).To(BeNil())

				Expect(err).To(Equal(KeyNotFoundErr))
			})
		})

		Context("when db returns an unexpected error", func() {
			It("returns the error as is", func() {
				mock.ExpectQuery(`SELECT \* FROM`).
					WillReturnError(fmt.Errorf("foo"))

				entry, err := rl.getExistingByName(newLockName)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("foo"))
				Expect(entry).To(BeNil())
			})
		})
	})

	Describe("isValid", func() {
		var (
			existingLock *LockEntry
		)

		BeforeEach(func() {
			existingLock = &LockEntry{
				ID:        1,
				Name:      existingLockName,
				Owner:     existingLockOwner,
				InUse:     true,
				LastError: "",
				LastUsed:  time.Now(),
				CreatedAt: time.Now(),
			}
		})

		Context("when given an existing lock that is NOT expired and still in use", func() {
			It("should return nil", func() {
				err := isValid(existingLock, existingLockName, 15*time.Minute)

				Expect(err).To(BeNil())
			})
		})

		Context("when given an existing lock that is stale (past MaxAge)", func() {
			It("should return error saying that the lock is stale", func() {
				existingLock.LastUsed = existingLock.LastUsed.AddDate(-1, 0, 0)

				err := isValid(existingLock, existingLockName, 15*time.Minute)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("lock is stale"))
			})
		})

		Context("when given an existing lock that is no longer in use", func() {
			It("should return an error saying that the lock is no longer in use", func() {
				existingLock.InUse = false

				err := isValid(existingLock, existingLockName, 15*time.Minute)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("lock is not in use"))

			})
		})

		Context("when existing lock is nil", func() {
			It("should return an error", func() {
				err := isValid(nil, "", 15*time.Minute)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("cannot be nil"))
			})
		})
	})

	Describe("Unlock", func() {
		var (
			l    *Lock
			mock sqlmock.Sqlmock
			rl   *RLock
		)

		BeforeEach(func() {
			_, mock, rl = setupMocks()

			l = &Lock{
				rl:      rl,
				name:    newLockName,
				timeout: 15 * time.Minute,
			}
		})

		Context("when unlocking a lock", func() {
			Context("with no error", func() {
				It("should update lock's last_error column with blank string AND return nil", func() {
					mock.ExpectExec(
						fmt.Sprintf(`^UPDATE %v SET in_use=0, last_error=.+\s+WHERE name=.+\s+AND owner=.+$`, TableName)).
						WithArgs("", l.name, l.rl.owner).
						WillReturnResult(sqlmock.NewResult(1, 1))

					err := l.Unlock(nil)

					Expect(err).ToNot(HaveOccurred())
					Expect(mock.ExpectationsWereMet()).ToNot(HaveOccurred())
				})
			})

			Context("with an error", func() {
				It("should update lock's last_error column with contents of error AND return nil", func() {
					unlockErr := "some error"

					mock.ExpectExec(
						fmt.Sprintf(`^UPDATE %v SET in_use=0, last_error=.+\s+WHERE name=.+\s+AND owner=.+$`, TableName)).
						WithArgs(unlockErr, l.name, l.rl.owner).
						WillReturnResult(sqlmock.NewResult(1, 1))

					err := l.Unlock(fmt.Errorf(unlockErr))

					Expect(err).ToNot(HaveOccurred())
					Expect(mock.ExpectationsWereMet()).ToNot(HaveOccurred())
				})
			})
		})

		Context("when update query fails", func() {
			It("should return error", func() {
				mock.ExpectExec(
					fmt.Sprintf(`^UPDATE %v SET in_use=0, last_error=.+\s+WHERE name=.+\s+AND owner=.+$`, TableName)).
					WillReturnError(fmt.Errorf("some error"))

				err := l.Unlock(nil)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unable to unlock"))
				Expect(err.Error()).To(ContainSubstring("some error"))
			})
		})

		Context("when unable to determine affected rows", func() {
			It("should return error", func() {
				mock.ExpectExec(
					fmt.Sprintf(`^UPDATE %v SET in_use=0, last_error=.+\s+WHERE name=.+\s+AND owner=.+$`, TableName)).
					WillReturnResult(sqlmock.NewErrorResult(fmt.Errorf("affected broke")))

				err := l.Unlock(nil)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unable to determine affected rows after unlock fo"))
				Expect(mock.ExpectationsWereMet()).ToNot(HaveOccurred())
			})
		})

		Context("when update query affected anything other than 1 row", func() {
			It("should return error", func() {
				mock.ExpectExec(
					fmt.Sprintf(`^UPDATE %v SET in_use=0, last_error=.+\s+WHERE name=.+\s+AND owner=.+$`, TableName)).
					WillReturnResult(sqlmock.NewResult(1, 2))

				err := l.Unlock(nil)

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unexpected number of affected rows after unlock "))
				Expect(mock.ExpectationsWereMet()).ToNot(HaveOccurred())
			})
		})
	})

	Describe("LastError", func() {
		var (
			l    *Lock
			mock sqlmock.Sqlmock
			rl   *RLock
		)

		BeforeEach(func() {
			_, mock, rl = setupMocks()

			l = &Lock{
				rl:      rl,
				name:    newLockName,
				timeout: acquireTimeout,
			}
		})

		Context("given an existing lock and a blank last_error", func() {
			It("returns nil", func() {
				rows := sqlmock.NewRows([]string{"last_error"}).AddRow("")

				mock.ExpectQuery("SELECT last_error").
					WithArgs(newLockName, rl.owner).
					WillReturnRows(rows)

				err := l.LastError()

				Expect(err).ToNot(HaveOccurred())
				Expect(mock.ExpectationsWereMet()).ToNot(HaveOccurred())
			})
		})

		Context("given an existing lock and a filled out last_error", func() {
			It("returns an error containing last_error", func() {
				rows := sqlmock.NewRows([]string{"last_error"}).AddRow("foo")

				mock.ExpectQuery("SELECT last_error").
					WithArgs(newLockName, rl.owner).
					WillReturnRows(rows)

				err := l.LastError()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("foo"))
				Expect(mock.ExpectationsWereMet()).ToNot(HaveOccurred())
			})
		})

		Context("when query execution fails", func() {
			It("returns an error", func() {
				mock.ExpectQuery("SELECT last_error").WillReturnError(fmt.Errorf("something broke"))

				err := l.LastError()

				Expect(err).To(HaveOccurred())
				Expect(err.Error()).To(ContainSubstring("unexpected error"))
				Expect(err.Error()).To(ContainSubstring("something broke"))
				Expect(mock.ExpectationsWereMet()).ToNot(HaveOccurred())
			})
		})
	})
})
