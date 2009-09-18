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
package org.apache.activemq.protobuf.compiler;

import java.io.File;
import java.io.FileFilter;
import java.util.Arrays;
import java.util.List;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.taskdefs.MatchingTask;

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ProtoTask extends MatchingTask {

    /**
     * The directory where the proto files (<code>*.proto</code>) are
     * located.
     */
    private File source;

    /**
     * The directory where the output files will be located.
     */
    private File target;

    /**
     * The type of generator to run.
     */
    private String type="alt";

    public void execute() throws BuildException {

        File[] protoFiles = null;
        if ( !source.exists() ) {
             throw new BuildException("source directory does not exist: "+ source);
        }

        if( source.isDirectory() ) {
            protoFiles = source.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.getName().endsWith(".proto");
                }
            });
            if (protoFiles==null || protoFiles.length==0) {
                throw new BuildException("No proto files found in directory: " + source.getPath());
            }
        } else {
            protoFiles = new File[]{source};
        }

        processFiles(protoFiles, target);
    }

    private void processFiles(File[] mainFiles, File outputDir) throws BuildException {
        List<File> recFiles = Arrays.asList(mainFiles);
        for (File file : recFiles) {
            try {
                log("Compiling: "+file.getPath());
                if( "default".equals(type) ) {
                    JavaGenerator generator = new JavaGenerator();
                    generator.setOut(outputDir);
                    generator.compile(file);
                } else if( "alt".equals(type) ) {
                    AltJavaGenerator generator = new AltJavaGenerator();
                    generator.setOut(outputDir);
                    generator.compile(file);
                }
            } catch (CompilerException e) {
                log("Protocol Buffer Compiler failed with the following error(s):");
                for (String error : e.getErrors() ) {
                    System.err.println("");
                    System.err.println(error);
                }
                System.err.println("");
                throw new BuildException("Compile failed.  For more details see error messages listed above.", e);
            }
        }
    }

    public File getSource() {
        return source;
    }

    public void setSource(File source) {
        this.source = source;
    }

    public File getTarget() {
        return target;
    }

    public void setTarget(File target) {
        this.target = target;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }
}