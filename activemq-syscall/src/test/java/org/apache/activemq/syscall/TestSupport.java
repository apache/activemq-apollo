package org.apache.activemq.syscall;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;

public class TestSupport {
    public static String generateString(int size) {
        StringBuffer sb = new StringBuffer();
        for( int i=0; i < size; i++ ) {
            sb.append((char)('a'+(i%26)));
        }
        String expected = sb.toString();
        return expected;
    }
    
    public static File dataFile(String name) {
        name.replace('/', File.separatorChar);
        String basedir = System.getProperty("basedir", "."); 
        File f = new File(basedir);
        f = new File(f, "target");
        f = new File(f, "test-data");
        f.mkdirs();
        return new File(f, name);
    }
    
    static public String readFile(File file) throws FileNotFoundException, IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        FileInputStream is = new FileInputStream(file);
        try {
            int c=0;
            while( (c=is.read())>=0 ) {
                baos.write(c);
            }
        } finally {
            is.close();
        }
        String actual = new String(baos.toByteArray());
        return actual;
    }

    static public void writeFile(File file, String content) throws FileNotFoundException, IOException {
        FileOutputStream os = new FileOutputStream(file);
        try {
            os.write(content.getBytes());
        } finally {
            os.close();
        }
    }
    
}
