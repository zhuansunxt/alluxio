package alluxio.master.file.meta;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class InodeRWLock {

  // Keep track of count of threads holding this RW lock.
  private AtomicInteger mReferenceCount;

  private ReentrantReadWriteLock mLock;

  InodeRWLock() {
    mReferenceCount = new AtomicInteger(0);
    mLock = new ReentrantReadWriteLock();
  }

  public String toString() {
    String str = "[";
    str += mLock.toString();
    str += ",";
    str += mReferenceCount.toString();
    str += "]";

    return str;
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

  public void incrementReferenceCount() {
    mReferenceCount.getAndIncrement();
  }

  public void decrementReferenceCount() {
    mReferenceCount.getAndDecrement();
  }

  public int getReferenceCount() {
    return mReferenceCount.get();
  }

  public boolean compareAndSetReferenceCount(int expect, int update) {
    return mReferenceCount.compareAndSet(expect, update);
  }
}
