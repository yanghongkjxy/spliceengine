package org.apache.hadoop.fs;

import org.junit.Assert;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.net.URI;

/**
 * Created by jleach on 11/9/16.
 */
public class EHFileSystemTest {
    private static String DIRECTORY = "/efs";
    private static String FILE = "/efs/foo.txt";

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
            System.out.println(fileSystem);
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
