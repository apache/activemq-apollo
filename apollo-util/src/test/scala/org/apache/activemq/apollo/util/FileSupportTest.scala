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
 
package org.apache.activemq.apollo.util

import org.scalatest.matchers.ShouldMatchers
import tools.nsc.io.{Path, File, Directory}
import collection.mutable.ArrayBuffer

class FileSupportTest extends FunSuiteSupport with ShouldMatchers {

  test("recursive file copy test") {

    val base = new Directory(baseDir)
    var target = base / FileSupport.toDirectory("target")

    val sourceDir: Directory = target / FileSupport.toDirectory("sourceDir")
    if ( sourceDir.exists ) {
      sourceDir.deleteRecursively
    }
    sourceDir.createDirectory(false)

    val subDir: Directory = sourceDir / FileSupport.toDirectory("subDir")
    subDir.createDirectory(false)

    val someFile: File = subDir / FileSupport.toFile("someFile")
    someFile.createFile(false)

    val targetDir: Directory = target / FileSupport.toDirectory("targetDir")
    if ( targetDir.exists ) {
      targetDir.deleteRecursively
    }

    FileSupport.recursiveCopy(sourceDir, targetDir)

    val listing = new ArrayBuffer[String]

    targetDir.deepList().foreach(file => listing.append(file.toString))

    listing should contain("./target/targetDir/subDir")
    listing should contain("./target/targetDir/subDir/someFile")

  }

  
}