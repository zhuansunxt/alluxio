package alluxio.master.file.meta;

import javax.annotation.concurrent.ThreadSafe;
import io.netty.util.internal.chmv8.ConcurrentHashMapV8;

/**
 * A Singleton of global inode lock manager.
 *
 * This singleton uses lazy initialization.
 */
@ThreadSafe
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

  // TODO: implement this.
  public InodeRWLock getLock(long inodeID) {
    return null;
  }

  // TODO: implement this.
  public void returnLock(long inodeID) {
  }
}
