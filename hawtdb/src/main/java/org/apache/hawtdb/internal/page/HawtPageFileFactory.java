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
package org.apache.hawtdb.internal.page;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class HawtPageFileFactory extends PageFileFactory {

    private HawtPageFile concurrentPageFile;
    
    protected boolean drainOnClose;
    protected boolean sync = true;
    protected boolean useWorkerThread;

    public HawtPageFileFactory() {
        super.setHeaderSize(HawtPageFile.FILE_HEADER_SIZE);
    }
    
    @Override
    public void setHeaderSize(int headerSize) {
        throw new IllegalArgumentException("headerSize property cannot not be manually configured.");
    }

    public void open() {
        if( file ==  null ) {
            throw new IllegalArgumentException("file property not set");
        }
        boolean existed = file.isFile();
        super.open();
        if (concurrentPageFile == null) {
            concurrentPageFile = new HawtPageFile(this);
            if( existed ) {
                concurrentPageFile.recover();
            } else {
                concurrentPageFile.reset();
            }
        }
    }
    
    public void close() {
        if (concurrentPageFile != null) {
            concurrentPageFile.suspend(true, false, drainOnClose);
            concurrentPageFile.flush();
            concurrentPageFile.performBatches();
            concurrentPageFile=null;
        }
        super.close();
    }

    public boolean isSync() {
        return sync;
    }

    public void setSync(boolean sync) {
        this.sync = sync;
    }
    
    public HawtPageFile getConcurrentPageFile() {
        return concurrentPageFile;
    }

    public boolean isDrainOnClose() {
        return drainOnClose;
    }

    public void setDrainOnClose(boolean drainOnClose) {
        this.drainOnClose = drainOnClose;
    }

    public boolean isUseWorkerThread() {
        return useWorkerThread;
    }

    public void setUseWorkerThread(boolean useWorkerThread) {
        this.useWorkerThread = useWorkerThread;
    }
    
}
