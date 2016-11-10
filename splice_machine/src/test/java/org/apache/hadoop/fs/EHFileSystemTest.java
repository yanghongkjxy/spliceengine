package org.apache.hadoop.fs;

import org.apache.hadoop.conf.Configuration;
import org.junit.Test;

import java.net.URI;

/**
 * Created by jleach on 11/9/16.
 */
public class EHFileSystemTest {

    @Test
    public void testEFSFileSystem() throws Exception {

        FileSystem fileSystem = FileSystem.get(new URI("file:///"),new Configuration());

        FsStatus fileStatus = fileSystem.getStatus(new Path("file:/efs/yarn/nm"));
        System.out.println(fileStatus.getCapacity());
        System.out.println(fileStatus.getRemaining());
        System.out.println(fileStatus.getUsed());

    }
}
