package alluxio.master.file.meta;

import java.util.ArrayList;
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

  private int getHash(int inodeID, int depth) {
    return inodeID % lockTable.get(depth).size();
  }

  public void lock(Inode inode, LockMode lockMode) {
    int depth = (int)inode.getParentId();     // TODO(xiaotong): change method.

    if (depth >= lockTable.size()) {
      if (depth > lockTable.size()) {
        // inode's depth should only be equal to or less than lock table's size.
        throw new IllegalArgumentException("Inode's depth is too large");
      }

      // Add another layer of the lock. Ensure the atomicity of the adding.
      tableLock.lock();
      try {
        lockTable.add(new ArrayList());
        int lockCount;
        if (depth == 0) {
          lockCount = 1;
        } else {
          lockCount = depth * depth;
        }
        for (int i = 0; i < lockCount; i++) {
          lockTable.get(depth).add(new ReentrantReadWriteLock());
        }
      } finally {
        tableLock.unlock();
      }
    }

    int hash = getHash((int)inode.getId(), depth);
    if (lockMode == LockMode.READ) {
      lockTable.get(depth).get(hash).readLock().lock();
    } else {
      lockTable.get(depth).get(hash).writeLock().lock();
    }
  }

  public void unlock(Inode inode, LockMode lockMode) {
    int depth = (int)inode.getParentId();     // TODO(xiaotong): change mothod.
    int hash = getHash((int)inode.getId(), depth);

    if (lockMode == LockMode.READ) {
      lockTable.get(depth).get(hash).readLock().unlock();
    } else {
      lockTable.get(depth).get(hash).writeLock().unlock();
    }
  }

  public boolean isReadLocked(Inode inode) {
    int depth = (int)inode.getParentId();     // TODO(xiaotong): change method.
    if (depth >= lockTable.size()) {
      return false;
    }

    int hash = getHash((int)inode.getId(), depth);
    return lockTable.get(depth).get(hash).getReadHoldCount() > 0;
  }

  public boolean isWriteLocked(Inode inode) {
    int depth = (int)inode.getParentId();     // TODO(xiaotong): change method.
    if (depth >= lockTable.size()) {
      return false;
    }

    int hash = getHash((int)inode.getId(), depth);
    return lockTable.get(depth).get(hash).isWriteLockedByCurrentThread();

  }
}