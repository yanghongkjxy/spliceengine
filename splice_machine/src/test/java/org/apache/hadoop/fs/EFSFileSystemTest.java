package org.apache.hadoop.fs;

import com.splicemachine.derby.test.framework.SpliceUnitTest;
import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import java.net.URI;

/**
 *
 * Testing the
 *
 *
 */
public class EFSFileSystemTest {
    private static String DIRECTORY = SpliceUnitTest.getBaseDirectory()+"/src/main/java/org/apache/hadoop/fs/EFSFileSystem.java";
    private static String FILE = SpliceUnitTest.getBaseDirectory()+"/src/main/java/org/apache/hadoop/fs/";

    @Test
    public void getStatus() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("efs:///"),new Configuration());

        FsStatus fileStatus = fileSystem.getStatus(new Path(DIRECTORY));
        Assert.assertTrue("capacity less than remaining",fileStatus.getCapacity()>fileStatus.getRemaining());
        Assert.assertTrue("no negative numbers",fileStatus.getCapacity()>0 &&
                fileStatus.getRemaining() > 0 && fileStatus.getUsed() >0);
    }

    @Test
    public void getContentSummary() throws Exception {
        try {
            FileSystem fileSystem = FileSystem.get(new URI("efs:///"), new Configuration());
            ContentSummary contentSummary = fileSystem.getContentSummary(new Path(DIRECTORY));
            contentSummary.getFileCount();
        } catch (Exception e) {
            e.printStackTrace();
            throw e;
        }
    }

    @Test
    public void getFileStatus() throws Exception {
        FileSystem fileSystem = FileSystem.get(new URI("efs:///"),new Configuration());
        FileStatus fs = fileSystem.getFileStatus(new Path(FILE));
        fs.getOwner();
        fs.getBlockSize();
    }
}
