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

  private int getHash(long inodeID, int depth) {
    return (int)(inodeID % lockTable.get(depth).size());
  }

  public boolean inorder(long id0, long id1,  int depth) {
    return getHash(id0, depth) < getHash(id1, depth);
  }

  public void lock(Inode inode, LockMode lockMode) {
    int depth = (int)inode.getDepth();

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
    int hash = getHash(inode.getId(), depth);

//    System.out.printf("id %d, hash %d, depth %d%n",inode.getId(), hash, depth );
    if (lockMode == LockMode.READ) {
      lockTable.get(depth).get(hash).readLock().lock();
    } else {
      lockTable.get(depth).get(hash).writeLock().lock();
    }
  }

  public void unlock(Inode inode, LockMode lockMode) {
    int depth = (int)inode.getDepth();
    int hash = getHash(inode.getId(), depth);

    if (lockMode == LockMode.READ) {
      lockTable.get(depth).get(hash).readLock().unlock();
    } else {
      lockTable.get(depth).get(hash).writeLock().unlock();
    }
  }

  public boolean isReadLocked(Inode inode) {
    int depth = (int)inode.getDepth();
    if (depth >= lockTable.size()) {
      return false;
    }

    int hash = getHash(inode.getId(), depth);
    return lockTable.get(depth).get(hash).getReadHoldCount() > 0;
  }

  public boolean isWriteLocked(Inode inode) {
    int depth = (int)inode.getDepth();
    if (depth >= lockTable.size()) {
      return false;
    }

    int hash = getHash(inode.getId(), depth);
    return lockTable.get(depth).get(hash).isWriteLockedByCurrentThread();

  }
}