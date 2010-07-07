package org.apache.activemq.syscall.jni;

import java.io.File;
import java.io.IOException;

import org.junit.Test;

import static org.apache.activemq.syscall.TestSupport.*;
import static org.apache.activemq.syscall.jni.CLibrary.*;
import static org.apache.activemq.syscall.jni.IO.*;
import static org.hamcrest.CoreMatchers.*;
import static org.junit.Assert.*;
import static org.junit.Assume.*;

public class IOTest {

    @Test
    public void fcntl_GETFL() throws IOException, InterruptedException {
        assumeThat(HAVE_FCNTL_FUNCTION, is(true));
        File file = dataFile(IOTest.class.getName() + ".direct.data");
        int fd = 0;
        try {
            // open the file...
            int oflags = O_CREAT | O_WRONLY | O_APPEND;
            int mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
            fd = open(file.getCanonicalPath(), oflags, mode);
            checkrc(fd);

            int rc = fcntl(fd, F_GETFL);
            checkrc(rc);
            assertTrue((rc & O_APPEND) != 0);

        } finally {
            checkrc(close(fd));
        }
    }

    private void checkrc(int rc) {
        if (rc == -1) {
            fail("IO failure: " + string(strerror(errno())));
        }
    }

    @Test
    public void testFree() {
        long ptr = malloc(100);
        free(ptr);
    }

}
