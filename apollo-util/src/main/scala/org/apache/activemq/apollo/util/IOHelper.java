/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.apollo.util;

import java.io.*;
import java.lang.reflect.Field;

import org.apache.activemq.apollo.util.os.CLibrary;
import org.apache.activemq.apollo.util.os.Kernel32Library;
import org.fusesource.hawtbuf.ByteArrayOutputStream;
import org.fusesource.hawtbuf.HexSupport;

/**
 */
public final class IOHelper {
    protected static final int MAX_DIR_NAME_LENGTH;
    protected static final int MAX_FILE_NAME_LENGTH;
    private static final int DEFAULT_BUFFER_SIZE = 4096;
    private IOHelper() {
    }

    public static String getDefaultDataDirectory() {
        return getDefaultDirectoryPrefix() + "activemq-data";
    }

    public static String getDefaultStoreDirectory() {
        return getDefaultDirectoryPrefix() + "amqstore";
    }

    /**
     * Allows a system property to be used to overload the default data
     * directory which can be useful for forcing the test cases to use a target/
     * prefix
     */
    public static String getDefaultDirectoryPrefix() {
        try {
            return System.getProperty("org.apache.activemq.default.directory.prefix", "");
        } catch (Exception e) {
            return "";
        }
    }

    /**
     * Converts any string into a string that is safe to use as a file name.
     * The result will only include ascii characters and numbers, and the "-","_", and "." characters.
     *
     * @param name
     * @return
     */
    public static String toFileSystemDirectorySafeName(String name) {
        return toFileSystemSafeName(name, true, MAX_DIR_NAME_LENGTH);
    }
    
    public static String toFileSystemSafeName(String name) {
        return toFileSystemSafeName(name, false, MAX_FILE_NAME_LENGTH);
    }
    
    /**
     * Converts any string into a string that is safe to use as a file name.
     * The result will only include ascii characters and numbers, and the "-","_", and "." characters.
     *
     * @param name
     * @param dirSeparators 
     * @param maxFileLength 
     * @return
     */
    public static String toFileSystemSafeName(String name,boolean dirSeparators,int maxFileLength) {
        int size = name.length();
        StringBuffer rc = new StringBuffer(size * 2);
        for (int i = 0; i < size; i++) {
            char c = name.charAt(i);
            boolean valid = c >= 'a' && c <= 'z';
            valid = valid || (c >= 'A' && c <= 'Z');
            valid = valid || (c >= '0' && c <= '9');
            valid = valid || (c == '_') || (c == '-') || (c == '.') || (c=='#')
                    ||(dirSeparators && ( (c == '/') || (c == '\\')));

            if (valid) {
                rc.append(c);
            } else {
                // Encode the character using hex notation
                rc.append('#');
                rc.append(HexSupport.toHexFromInt(c, true));
            }
        }
        String result = rc.toString();
        if (result.length() > maxFileLength) {
            result = result.substring(result.length()-maxFileLength,result.length());
        }
        return result;
    }
    
    public static boolean deleteFile(File fileToDelete) {
        if (fileToDelete == null || !fileToDelete.exists()) {
            return true;
        }
        boolean result = deleteChildren(fileToDelete);
        result &= fileToDelete.delete();
        return result;
    }
    
    public static boolean deleteChildren(File parent) {
        if (parent == null || !parent.exists()) {
            return false;
        }
        boolean result = true;
        if (parent.isDirectory()) {
            File[] files = parent.listFiles();
            if (files == null) {
                result = false;
            } else {
                for (int i = 0; i < files.length; i++) {
                    File file = files[i];
                    if (file.getName().equals(".")
                            || file.getName().equals("..")) {
                        continue;
                    }
                    if (file.isDirectory()) {
                        result &= deleteFile(file);
                    } else {
                        result &= file.delete();
                    }
                }
            }
        }
       
        return result;
    }
    
    
    public static void moveFile(File src, File targetDirectory) throws IOException {
        if (!src.renameTo(new File(targetDirectory, src.getName()))) {
            throw new IOException("Failed to move " + src + " to " + targetDirectory);
        }
    }
    
    public static void copyFile(File src, File dest) throws IOException {
        FileInputStream fileSrc = new FileInputStream(src);
        FileOutputStream fileDest = new FileOutputStream(dest);
        copyInputStream(fileSrc, fileDest);
    }

    public static String readText(File path) throws IOException {
        return readText(path, "UTF-8");
    }
    public static String readText(File path, String encoding) throws IOException {
      return readText(new FileInputStream(path), encoding);
    }

    public static String readText(InputStream in) throws IOException {
        return readText(in, "UTF-8");
    }

    public static String readText(InputStream in, String encoding) throws IOException {
      return new String(readBytes(in), encoding);
    }

    public static byte[] readBytes(File path) throws IOException {
      return readBytes(new FileInputStream(path));
    }

    public static byte[] readBytes(InputStream in) throws IOException {
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      copyInputStream(in, baos);
      return baos.toByteArray();
    }

    public static void writeText(File path, String text) throws IOException {
        writeText(path, text, "UTF-8");
    }

    public static void writeText(File path, String text, String encoding) throws IOException {
      OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(path), encoding);
      try {
        out.write(text);
      } finally {
        close(out);
      }
    }

    public static void writeBinaryFile(File path, byte[] contents) throws IOException {
      FileOutputStream out = new FileOutputStream(path);
      try {
        out.write(contents);
      } finally {
        close(out);
      }
    }

    public static void copyInputStream(InputStream in, OutputStream out) throws IOException {
        try {
            byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
            int len = in.read(buffer);
            while (len >= 0) {
                out.write(buffer, 0, len);
                len = in.read(buffer);
            }
        } finally {
            close(in);
            close(out);
        }
    }

    public static void close(Writer out) {
        try {
            out.close();
        } catch (IOException e) {
        }
    }

    public static void close(OutputStream out) {
        try {
            out.close();
        } catch (IOException e) {
        }
    }

    public static void close(InputStream in) {
        try {
            in.close();
        } catch (IOException e) {
        }
    }

    static {
        MAX_DIR_NAME_LENGTH = Integer.valueOf(System.getProperty("MaximumDirNameLength","200")).intValue();  
        MAX_FILE_NAME_LENGTH = Integer.valueOf(System.getProperty("MaximumFileNameLength","64")).intValue();             
    }

    
    public static void mkdirs(File dir) throws IOException {
        if (dir.exists()) {
            if (!dir.isDirectory()) {
                throw new IOException("Failed to create directory '" + dir +"', regular file already existed with that name");
            }
            
        } else {
            if (!dir.mkdirs()) {
                throw new IOException("Failed to create directory '" + dir+"'");
            }
        }
    }
    
	public interface SyncStrategy {
		void sync(FileDescriptor fdo) throws IOException;
	}
    static final SyncStrategy SYNC_STRATEGY = createSyncStrategy();
    static public void sync(FileDescriptor fd) throws IOException {
        SYNC_STRATEGY.sync(fd);
    }

	private static SyncStrategy createSyncStrategy() {
		
		// On OS X, the fsync system call does not fully flush the hardware buffers.. 
		// to do that you have to do an fcntl call, and the only way to do that is to
		// do some JNI.  
		String os = System.getProperty("os.name");
		if( "Mac OS X".equals(os) ) {

			// We will gracefully fall back to default JDK file sync behavior
			// if the JNA library is not in the path, and we can't set the 
			// FileDescriptor.fd field accessible.
			try {
				final Field field = FileDescriptor.class.getDeclaredField("fd");
				field.setAccessible(true);
				// Try to dynamically load the JNA impl of the CLibrary interface..
				final CLibrary lib = getCLibrary();
				return new SyncStrategy() {
					static final int F_FULLFSYNC = 51;	
					public void sync(FileDescriptor fd) throws IOException {
						try {
							int id = field.getInt(fd);
							lib.fcntl(id, F_FULLFSYNC);
						} catch (Exception e) {
							throw IOExceptionSupport.create(e);
						}
					}

                    public void hardlink(File source, File target) throws IOException {
                        int rc = lib.link(source.getCanonicalPath(), target.getCanonicalPath());
                        if( rc != 0 ){
                            throw new IOException("Hard link failed with result code="+rc);
                        }
                    }
                };
			} catch (Throwable ignore) {
				// Perhaps we should issue a warning here so folks know that 
				// the disk syncs are not going to be of very good quality.
			}
		} else if( os.toLowerCase().startsWith("windows") ) {
            // We will gracefully fall back to default JDK file sync behavior
            // if the JNA library is not in the path, and we can't set the
            // FileDescriptor.fd field accessible.
            try {
                final Kernel32Library lib = getKernel32Library();
                return new SyncStrategy() {
                    public void sync(FileDescriptor fd) throws IOException {
                        fd.sync();
                    }
                    public void hardlink(File source, File target) throws IOException {
                        int rc = lib.CreateHardLink(target.getCanonicalPath(), source.getCanonicalPath(), 0);
                        if( rc == 0 ){
                            throw new IOException("Hard link failed with result code="+lib.GetLastError());
                        }
                    }
                };
            } catch (Throwable ignore) {
                // Perhaps we should issue a warning here so folks know that
                // the disk syncs are not going to be of very good quality.
            }
        }


        // We will gracefully fall back to default JDK file sync behavior
        // if the JNA library is not in the path, and we can't set the
        // FileDescriptor.fd field accessible.
        try {
            final CLibrary lib = getCLibrary();
            return new SyncStrategy() {
                public void sync(FileDescriptor fd) throws IOException {
                    fd.sync();
                }

                public void hardlink(File source, File target) throws IOException {
                    int rc = lib.link(source.getCanonicalPath(), target.getCanonicalPath());
                    if( rc != 0 ){
                        throw new IOException("Hard link failed with result code="+rc);
                    }
                }
            };
        } catch (Throwable ignore) {
            // Perhaps we should issue a warning here so folks know that
            // the disk syncs are not going to be of very good quality.
        }

		return new SyncStrategy() {
			public void sync(FileDescriptor fd) throws IOException {
				fd.sync();
			}

            public void hardlink(File source, File target) throws IOException {

            }
        };
	}

    public interface HardLinkStrategy {
        void hardlink(File source, File target) throws IOException;
    }
    static final HardLinkStrategy HARD_LINK_STRATEGY = createHardLinkStrategy();
    static public void hardlink(File source, File target) throws IOException {
        if(HARD_LINK_STRATEGY==null)
            throw new UnsupportedOperationException();
        HARD_LINK_STRATEGY.hardlink(source, target);
    }

    private static HardLinkStrategy createHardLinkStrategy() {

        String os = System.getProperty("os.name");
        if( os.toLowerCase().startsWith("windows") ) {
            try {
                final Kernel32Library lib = getKernel32Library();
                return new HardLinkStrategy() {
                    public void hardlink(File source, File target) throws IOException {
                        int rc = lib.CreateHardLink(target.getCanonicalPath(), source.getCanonicalPath(), 0);
                        if( rc == 0 ){
                            throw new IOException("Hard link failed with result code="+lib.GetLastError());
                        }
                    }
                };
            } catch (Throwable ignore) {
            }
        }

        try {
            final CLibrary lib = getCLibrary();
            return new HardLinkStrategy() {
                public void hardlink(File source, File target) throws IOException {
                    int rc = lib.link(source.getCanonicalPath(), target.getCanonicalPath());
                    if( rc != 0 ){
                        throw new IOException("Hard link failed with result code="+rc);
                    }
                }
            };
        } catch (Throwable ignore) {
        }
        return null;
    }

	@SuppressWarnings("unchecked")
	public static CLibrary getCLibrary() throws ClassNotFoundException, IllegalAccessException, NoSuchFieldException {
		Class clazz = IOHelper.class.getClassLoader().loadClass("org.apache.activemq.apollo..util.os.JnaCLibrary");
		final CLibrary lib = (CLibrary) clazz.getField("INSTANCE").get(null);
		return lib;
	}

    @SuppressWarnings("unchecked")
    public static Kernel32Library getKernel32Library() throws ClassNotFoundException, IllegalAccessException, NoSuchFieldException {
        Class clazz = IOHelper.class.getClassLoader().loadClass("org.apache.activemq.apollo.util.os.Kernel32JnaLibrary");
        final Kernel32Library lib = (Kernel32Library) clazz.getField("INSTANCE").get(null);
        return lib;
    }

}
