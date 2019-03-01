[![CircleCI](https://circleci.com/gh/dselans/rlock.svg?style=svg)](https://circleci.com/gh/dselans/rlock)
[![codecov](https://codecov.io/gh/dselans/rlock/branch/master/graph/badge.svg?token=eWHfq11AM9)](https://codecov.io/gh/dselans/rlock)
[![Go Report Card](https://goreportcard.com/badge/github.com/dselans/rlock)](https://goreportcard.com/report/github.com/dselans/rlock)
[![Godocs](https://img.shields.io/badge/golang-documentation-blue.svg)](https://godoc.org/github.com/dselans/rlock)

## Overview
rlock is a "remote lock" lib that uses an SQL DB for its backend.

Think "remote mutex" with some [bells and whistles](#bells-and-whistles).

_WARNING: Using a non-distributed backend for a remote lock is **dangerous**!
If you are concerned about this, add support for a distributed backend and submit
a PR .. or re-evaluate your approach and do not use this library._ 

## Bells and Whistles
A basic remote mutex lock implementation is pretty simple: try to acquire a lock
by continuously trying to insert (or update) a lock record until you hit an error,
timeout or success.

On top of the above, `rlock` also enables the lock holder to pass state to any
potential _future_ lock owners via the `Unlock(err Error)` method.

When a future lock owner acquires a lock, it can check to see _what_ (if any) error
a previous lock owner ran into by using `LastError()`. By examining the error,
the future lock owner can determine if the previous lock owner ran into a fatal
error or an error that the current lock holder may potentially be able to avoid.

Neat!

## Use Case / Example Scenario
Imagine you have _ten_ instances of a service that are all load balanced. Each
one of these instances is able to create some sort of a resource that takes 1+ 
minutes to create.

1. Request A comes in and is load balanced to instance #1
1. Instance #1 checks if requested resource exists -- _it does not_
1. Instance #1 starts creating resource
1. Request B comes in and is load balanced to instance #2
1. Instance #2 checks if requested resource exists -- _it does not_ (because
it is being actively created by instance #1)
1. Instance #2 starts creating resource
1. **We have a race** -- Both #1 and #2 are creating the same resource that is
likely to result in a bad outcome

The above problem case can be mitigated by introducing a remote lock. Going off the 
previous scenario, the sequence of events would look something like this:

1. Request A comes in and is load balanced to instance #1
1. Instance #1 acquires lock via `Lock("MyLock", 2 * time.Minute)`
1. Instance #1 starts creating resource
1. Request B comes in and is load balanced to instance #2
1. Instance #2 attempts to acquire lock via `Lock("MyLock", 2 * time.Minute)`
1. Instance #2 blocks waiting on lock acquire until either:
    * IF instance #1 finishes work and unlocks "MyLock"
        1. Instance #2 acquires the lock
        1. Instance #2 checks if resource exists -- _IT DOES_
        1. Instance #2 avoids creating resource and moves on to next step
    * IF Instance #1 doesn't finish work and/or doesn't unlock "MyLock"
        1. Instance #2 receives an `AcquireTimeoutErr` and errors out
    * IF Instance #1 runs into a _recoverable_ error and unlocks "MyLock" but with
      an error (that instance #2 can look at and determine if it should re-attempt
      to do the "work" once more)
        1. Instance #2 acquires the Lock and checks to see if the previous lock
        user ran into an error via `LastError()`
        1. Instance #2 sees that the last lock user indeed ran into an error but
        instance #2 knows how to mitigate the error (for example, this could be a
        temporary network error that is likely to go away)
        1. Instance #2 attempts to create the resource AND succeeds
        1. Instance #2 releases the lock

## Contrived Example
1. Launch two goroutines
1. One goroutine is told to Unlock the lock WITH an error
1. Second goroutine is told to Unlock WITHOUT an error
1. Both goroutines check if previous lock owners ran into an error

```golang
import (
    "fmt"
    "os"
    "time"
    
    "github.com/dselans/rlock"
    _ "github.com/go-sql-driver/mysql"
    "github.com/jmoiron/sqlx"
)

var (
    AcquireTimeout   = 10 * time.Second
    RecoverableError = errors.New("recoverable error")
)
    
func main() {
    // Connect to a DB using sqlx
    db, _ := sqlx.Connect("mysql", "user:pass@tcp(127.0.0.1:3306)/dbname?parseTime=true"
    
    // Create an rlock instance
    rl, _ := rlock.New(db)
    
    go createResource(rl, RecoverableError)
    go createResource(rl, nil)
    
    time.Sleep(12 * time.Second)
    
    fmt.Printn("Done!")
}

func createResource(rl *rlock.Lock, stateError error) {
    l, _ := rl.Lock("MyLock", AcquireTimeout)
    lastError = l.LastError()
    
    if lastError != nil {
        if lastError == RecoverableError {
            // Do recovery work
        } else {
           // Fatal error, quit
           os.Exit(1)
        }
    }
    
    // Do actual work
    // ... 
    
    // Release lock
    l.Unlock(stateError)
}
```