package lock

import (
	"testing"
	"time"
)

func TestDistributedLockAcquireRelease(t *testing.T) {
	dl := NewDistributedLock()

	acquired, err := dl.Acquire("lock1", "owner1", 10*time.Second)
	if err != nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	if !acquired {
		t.Error("expected to acquire lock")
	}

	acquired2, _ := dl.Acquire("lock1", "owner2", 10*time.Second)
	if acquired2 {
		t.Error("expected second acquire to fail")
	}

	released := dl.Release("lock1", "owner1")
	if !released {
		t.Error("expected to release lock")
	}

	acquired3, _ := dl.Acquire("lock1", "owner2", 10*time.Second)
	if !acquired3 {
		t.Error("expected to acquire after release")
	}
}

func TestDistributedLockExpiry(t *testing.T) {
	dl := NewDistributedLock()

	_, _ = dl.Acquire("lock1", "owner1", 50*time.Millisecond)
	time.Sleep(100 * time.Millisecond)

	if dl.IsLocked("lock1") {
		t.Error("expected lock to be expired")
	}
}

func TestDistributedLockOwner(t *testing.T) {
	dl := NewDistributedLock()

	_, _ = dl.Acquire("lock1", "owner1", 10*time.Second)

	owner, err := dl.GetOwner("lock1")
	if err != nil {
		t.Fatalf("GetOwner failed: %v", err)
	}
	if owner != "owner1" {
		t.Errorf("expected owner1, got %s", owner)
	}
}

func TestDistributedLockCleanup(t *testing.T) {
	dl := NewDistributedLock()

	dl.Acquire("lock1", "owner1", 1*time.Millisecond)
	dl.Acquire("lock2", "owner2", 10*time.Second)
	time.Sleep(10 * time.Millisecond)

	cleaned := dl.Cleanup()
	if cleaned != 1 {
		t.Errorf("expected 1 cleaned, got %d", cleaned)
	}
}

func TestDistributedLockCount(t *testing.T) {
	dl := NewDistributedLock()

	dl.Acquire("lock1", "owner1", 10*time.Second)
	dl.Acquire("lock2", "owner2", 10*time.Second)

	if dl.LockCount() != 2 {
		t.Errorf("expected 2 locks, got %d", dl.LockCount())
	}
}
