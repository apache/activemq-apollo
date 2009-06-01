/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.project.MavenProject;

/**
 * A Maven Mojo so that the Proto compiler can be used with maven.
 * 
 * @goal compile
 * @phase process-sources
 */
public class ProtoMojo extends AbstractMojo {

    /**
     * The maven project.
     * 
     * @parameter expression="${project}"
     * @required
     * @readonly
     */
    protected MavenProject project;

    /**
     * The directory where the proto files (<code>*.proto</code>) are
     * located.
     * 
     * @parameter default-value="${basedir}/src/main/proto"
     */
    private File mainSourceDirectory;

    /**
     * The directory where the output files will be located.
     * 
     * @parameter default-value="${project.build.directory}/generated-sources/proto"
     */
    private File mainOutputDirectory;
    
    /**
     * The directory where the proto files (<code>*.proto</code>) are
     * located.
     * 
     * @parameter default-value="${basedir}/src/test/proto"
     */
    private File testSourceDirectory;

    /**
     * The directory where the output files will be located.
     * 
     * @parameter default-value="${project.build.directory}/test-generated-sources/proto"
     */
    private File testOutputDirectory;
    
    /**
     * The type of generator to run.
     * 
     * @parameter default-value="default"
     */
    private String type;

    public void execute() throws MojoExecutionException {

        File[] mainFiles = null;
        if ( mainSourceDirectory.exists() ) {
            mainFiles = mainSourceDirectory.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.getName().endsWith(".proto");
                }
            });
            if (mainFiles==null || mainFiles.length==0) {
                getLog().warn("No proto files found in directory: " + mainSourceDirectory.getPath());
            } else {
                processFiles(mainFiles, mainOutputDirectory);
                this.project.addCompileSourceRoot(mainOutputDirectory.getAbsolutePath());
            }
        }
        
        File[] testFiles = null;
        if ( testSourceDirectory.exists() ) {
            testFiles = testSourceDirectory.listFiles(new FileFilter() {
                public boolean accept(File pathname) {
                    return pathname.getName().endsWith(".proto");
                }
            });
            if (testFiles==null || testFiles.length==0) {
                getLog().warn("No proto files found in directory: " + testSourceDirectory.getPath());
            } else {
                processFiles(testFiles, testOutputDirectory);
                this.project.addTestCompileSourceRoot(testOutputDirectory.getAbsolutePath());
            }
        }
    }

    private void processFiles(File[] mainFiles, File outputDir) throws MojoExecutionException {
        List<File> recFiles = Arrays.asList(mainFiles);
        for (File file : recFiles) {
            try {
                getLog().info("Compiling: "+file.getPath());
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
                getLog().error("Protocol Buffer Compiler failed with the following error(s):");
                for (String error : e.getErrors() ) {
                    getLog().error("");
                    getLog().error(error);
                }
                getLog().error("");
                throw new MojoExecutionException("Compile failed.  For more details see error messages listed above.", e);
            }
        }
    }

}
