package alluxio.master.file.meta;

import io.netty.util.internal.chmv8.ConcurrentHashMapV8;


public class InodeLockManager {
  final private static ConcurrentHashMapV8<Long, InodeRWLock> mLockMap
          = new ConcurrentHashMapV8<>();

  private static InodeLockManager instance = new InodeLockManager();

  private InodeLockManager() {}

  public static InodeLockManager getInstance() {
    return instance;
  }

  // TODO: implement this.
  public static InodeRWLock getLock(long inodeID) {
    return mLockMap.get(inodeID);
  }

  // TODO: implement this.
  public static void returnLock(long inodeID) {
  }
}
