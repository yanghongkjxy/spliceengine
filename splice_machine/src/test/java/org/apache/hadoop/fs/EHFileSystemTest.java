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

        FileStatus fileStatus = fileSystem.getFileStatus(new Path("file:/efs/spark/local/spark-7a9cfcb1-f66b-4cac-b792-e4cff71a5a7f/__spark_conf__1761978367541521367.zip"));
        System.out.println(fileStatus.getOwner());
    }
}
