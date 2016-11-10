package org.apache.hadoop.fs;

/**
 * Created by jleach on 11/9/16.
 */
import com.google.common.annotations.VisibleForTesting;

import java.io.BufferedOutputStream;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.FileDescriptor;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.AccessDeniedException;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.StringTokenizer;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.nativeio.NativeIO;
import org.apache.hadoop.util.Progressable;
import org.apache.hadoop.util.Shell;
import org.apache.hadoop.util.StringUtils;

/****************************************************************
 * Implement the FileSystem API for the raw local filesystem.
 *
 *****************************************************************/
@InterfaceAudience.Public
@InterfaceStability.Stable
public class EFSFileSystem extends RawLocalFileSystem {
    static final URI NAME = URI.create("file:///");
    @Override
    public URI getUri() {
        return NAME;
    }

    @Override
    public String getScheme() {
        return "efs";
    }


    @Override
    public FsStatus getStatus(Path p) throws IOException {
        File partition = pathToFile(p == null ? new Path("/") : p);
        //File provides getUsableSpace() and getFreeSpace()
        //File provides no API to obtain used space, assume used = total - free
        return new FsStatus(Long.MAX_VALUE,
                125l,
                Long.MAX_VALUE-125l);
    }




}
