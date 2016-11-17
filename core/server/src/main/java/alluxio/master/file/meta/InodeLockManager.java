package alluxio.master.file.meta;

import javax.annotation.concurrent.ThreadSafe;

import com.google.common.base.Preconditions;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;

/**
 * A Singleton of global inode lock manager.
 *
 * This singleton uses lazy initialization.
 */
public final class InodeLockManager {
  private final ConcurrentHashMapV8<Long, InodeRWLock> sLockMap;

  private static InodeLockManager sInodeLockManager = null;

  // pvevent initialization
  private InodeLockManager() {
    if (sInodeLockManager != null)
      throw new IllegalStateException("Inode lock manager already initialized");
    sLockMap = new ConcurrentHashMapV8<>();
  }

  public static InodeLockManager get() {
    // local variable increases performance.
    InodeLockManager instance = sInodeLockManager;

    // double check for thread-safety and reducing contention.
    if (instance == null) {
      synchronized (InodeLockManager.class) {
        if (instance == null) {
          instance = sInodeLockManager = new InodeLockManager();
        }
      }
    }
    return instance;
  }

  // Thread safety ensured by locking.
  // TODO: make implementation lock-free.
  public synchronized InodeRWLock getLock(long inodeID) {
    InodeRWLock inodeLock;
    if ((inodeLock = sLockMap.get(Long.valueOf(inodeID))) != null) {
      inodeLock.incrementReferenceCount();
    } else {
      inodeLock = new InodeRWLock();
      sLockMap.put(Long.valueOf(inodeID), inodeLock);
    }
    return inodeLock;
  }

  // Thread safety ensured by locking.
  // TODO: make implementation lock-free.
  public void returnLock(long inodeID) {
    InodeRWLock inodeLock = sLockMap.get(Long.valueOf(inodeID));
    Preconditions.checkState(inodeLock != null,
            "Entry must exist for any Inode when returning the lock");

    inodeLock.decrementReferenceCount();
    if (inodeLock.getReferenceCount() == 0) {
      sLockMap.remove(Long.valueOf(inodeID));
    }
  }
}
