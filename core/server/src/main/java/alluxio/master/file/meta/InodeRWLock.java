package alluxio.master.file.meta;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InodeRWLock {
  private AtomicInteger mRefenceCount;
  private ReentrantReadWriteLock mLock;

  InodeRWLock() {
    mRefenceCount = new AtomicInteger();   // initial value is 0.
    mLock = new ReentrantReadWriteLock();
  }

  public void lockRead() {
    mLock.readLock().lock();
  }

  public void unLockRead() {
    mLock.readLock().unlock();
  }

  public void lockWrite() {
    mLock.writeLock().lock();
  }

  public void unlockWrite() {
    mLock.writeLock().unlock();
  }

  public boolean isWriteLockedByCurrentThread() {
    return mLock.isWriteLockedByCurrentThread();
  }

  public int getReadHoldCount() {
    return mLock.getReadHoldCount();
  }

  // TODO: make this method atomic.
  public void incrementReferenceCount() {
    mRefenceCount.getAndIncrement();
  }

  // TODO: make this method atomic.
  public void decrementReferenceCount() {
    mRefenceCount.getAndDecrement();
  }
}
