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

  // The current implementation of the getLock and returnLock interface uses
  // explicit locking to ensure the correctness and thread-safety. The next
  // step is to investigate a lock-free non-blocking approach to prevent from
  // performance degration. Some CAS hacks may be utilized.

  /**
   * Return an {@link InodeRWLock} to a requesting {@link Inode}
   *
   * Thread safety ensured by locking.
   */
  // TODO(lei): make implementation lock-free.
  public InodeRWLock getLock(Inode inode) {
    int ref = -1;
    InodeRWLock inodeLock;

    do {
      inodeLock = sLockMap.get(inode.mId);
      while(inodeLock == null) {
        sLockMap.putIfAbsent(inode.mId, new InodeRWLock());
        inodeLock = sLockMap.get(inode.mId);
      }
      ref = inodeLock.getReferenceCount();
    } while (ref == -1 || !inodeLock.compareAndSetReferenceCount(ref, ref+1));
    return inodeLock;
  }

  /**
   * Receive back an {@link InodeRWLock} from a {@link Inode}
   *
   * Thread safety ensured by locking.
   */
  // TODO(lei): make implementation lock-free.
  public void returnLock(Inode inode) {
    InodeRWLock inodeLock = sLockMap.get(inode.mId);
    Preconditions.checkState(inodeLock != null,
            "Entry must exist for any Inode when returning the lock");

    inodeLock.decrementReferenceCount();
    if (inodeLock.getReferenceCount() == 0) {
      if(inodeLock.compareAndSetReferenceCount(0, -1)) {
        sLockMap.remove(inode.mId);
      }
    }
  }
}
