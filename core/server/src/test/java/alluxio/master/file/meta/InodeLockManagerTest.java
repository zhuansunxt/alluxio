package alluxio.master.file.meta;


import alluxio.Constants;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Unit test for {@link InodeLockManager}
 */
public final class InodeLockManagerTest {
  private static final Logger LOG = LoggerFactory.getLogger(Constants.LOGGER_TYPE);

  /**
   * Test the singleton nature of {@link InodeLockManager}
   */
  @Test
  public void getSingleton() {
    InodeLockManager inodeLockManager = InodeLockManager.get();
    InodeLockManager inodeLockManager1 = InodeLockManager.get();
    Assert.assertTrue(inodeLockManager != null && inodeLockManager instanceof InodeLockManager);
    Assert.assertTrue(inodeLockManager == inodeLockManager1);
  }
}
