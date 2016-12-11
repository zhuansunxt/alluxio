package alluxio.master.file.meta;

import java.util.ArrayList;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public final class InodeLockManager {

  private static InodeLockManager sInodeLockManager = null;

  private final ArrayList<ArrayList<ReentrantReadWriteLock>> lockTable;
  private final ReentrantLock tableLock;

  public enum LockMode {
    READ,
    WRITE,
  }

  // prevent intialization.
  private InodeLockManager() {
    if (sInodeLockManager != null)
      throw new IllegalStateException("Inode lock manager already initialized");
    lockTable = new ArrayList();
    tableLock = new ReentrantLock();
  }

  // get method for InodeLockManager singleton.
  public static InodeLockManager get() {
    InodeLockManager instance = sInodeLockManager;

    if (instance == null) {
      synchronized (InodeLockManager.class) {
        if (instance == null) {
          instance = sInodeLockManager = new InodeLockManager();
        }
      }
    }
    return instance;
  }

  private int getHash(long inodeID, int depth) {
    return (int)(inodeID % lockTable.get(depth).size());
  }

  private Lock getLockFromTable(Inode inode, LockMode lockMode) {
    int depth = inode.getDepth();
    int hash = getHash(inode.getId(), depth);
    ReentrantReadWriteLock lock = lockTable.get(depth).get(hash);
    Lock rwlock = lockMode == LockMode.READ ? lock.readLock() : lock.writeLock();

    return rwlock;
  }

  public void lock(Inode inode, LockMode lockMode) {
    int depth = inode.getDepth();

    if (depth >= lockTable.size()) {
      if (depth > lockTable.size()) {
        // inode's depth should only be equal to or less than lock table's size.
        throw new IllegalArgumentException("Inode's depth is too large");
      }

      // Add another layer of the lock. Ensure the atomicity of the adding.
      tableLock.lock();
      try {
        if (depth == lockTable.size()) {
          lockTable.add(new ArrayList());
          int lockCount = Math.min(512, (depth+1) * (depth+1));
          for (int i = 0; i < lockCount; i++) {
            lockTable.get(depth).add(new ReentrantReadWriteLock());
          }
        }
      } finally {
        tableLock.unlock();
      }
    }

    getLockFromTable(inode, lockMode).lock();
  }

  public void unlock(Inode inode, LockMode lockMode) {
    getLockFromTable(inode, lockMode).unlock();
  }

  public void lockPair(Inode inode1, Inode inode2, LockMode lockMode1, LockMode lockMode2) {
    // assumption: two inodes' layer should not exceed the max layer in lock table.

    int hash1 = getHash(inode1.getId(), inode1.getDepth());
    int hash2 = getHash(inode2.getId(), inode2.getDepth());
    Lock rwlock1 = getLockFromTable(inode1, lockMode1);
    Lock rwlock2 = getLockFromTable(inode2, lockMode2);

    if (hash1 < hash2) {
      rwlock1.lock();
      rwlock2.lock();
    } else {
      rwlock2.lock();
      rwlock1.lock();
    }
  }

  public void unlockPair(Inode inode1, Inode inode2, LockMode lockMode1, LockMode lockMode2) {
    // assumption: two inodes' layer should not exceed the max layer in lock table.

    int hash1 = getHash(inode1.getId(), inode1.getDepth());
    int hash2 = getHash(inode2.getId(), inode2.getDepth());
    Lock rwlock1 = getLockFromTable(inode1, lockMode1);
    Lock rwlock2 = getLockFromTable(inode2, lockMode2);

    if (hash1 < hash2) {
      rwlock2.unlock();
      rwlock1.unlock();
    } else {
      rwlock1.unlock();
      rwlock2.unlock();
    }
  }

  public boolean isReadLocked(Inode inode) {
    int depth = inode.getDepth();
    if (depth >= lockTable.size()) {
      return false;
    }

    int hash = getHash(inode.getId(), depth);
    return lockTable.get(depth).get(hash).getReadHoldCount() > 0;
  }

  public boolean isWriteLocked(Inode inode) {
    int depth = inode.getDepth();
    if (depth >= lockTable.size()) {
      return false;
    }

    int hash = getHash(inode.getId(), depth);
    return lockTable.get(depth).get(hash).isWriteLockedByCurrentThread();

  }
}