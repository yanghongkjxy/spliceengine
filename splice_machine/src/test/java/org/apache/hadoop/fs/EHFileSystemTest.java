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

        FileStatus fileStatus = fileSystem.getFileStatus(new Path("file:/efs/spark/local/spark-f86cfee4-6dd8-4d36-88d9-c187381d0c05/__spark_conf__7178187324488808181.zip"));
        System.out.println(fileStatus.getOwner());
    }
}
