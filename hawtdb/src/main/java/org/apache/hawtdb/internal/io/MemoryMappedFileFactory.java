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
package org.apache.hawtdb.internal.io;

import java.io.File;
import java.io.IOException;

/**
 * Factory for {@link MemoryMappedFile} objects.
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class MemoryMappedFileFactory {
    
    private MemoryMappedFile memoryMappedFile;
    
    protected File file;
    protected int mappingSegementSize=1024*1024*64;
    
    public void open() throws IOException {
        if( memoryMappedFile == null ) {
            if( file ==  null ) {
                throw new IllegalArgumentException("file property not set");
            }
            if( mappingSegementSize <= 0 ) {
                throw new IllegalArgumentException("mappingSegementSize property must be greater than 0");
            }
            // We auto create the parent directory.
            file.getParentFile().mkdirs();
            memoryMappedFile = new MemoryMappedFile(file, mappingSegementSize);
        }
    }
    
    public void close() {
        if( memoryMappedFile!=null ) {
            memoryMappedFile.close();
            memoryMappedFile=null;
        }
    }

    public MemoryMappedFile getMemoryMappedFile() throws IOException {
        return memoryMappedFile;
    }

    public File getFile() {
        return file;
    }
    public void setFile(File file) {
        this.file = file;
    }

    public int getMappingSegementSize() {
        return mappingSegementSize;
    }
    public void setMappingSegementSize(int mappingSegementSize) {
        this.mappingSegementSize = mappingSegementSize;
    }
    
}
