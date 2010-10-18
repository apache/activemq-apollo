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

import tools.nsc.io.{File, Path, Directory}

object FileSupport {

  def recursiveCopy(source: Path, target: Path) : Unit = {
    require(source.isDirectory, source.path + " must be a directory.")
    if ( !target.exists ) {
      target.toDirectory.createDirectory()
    }

    def createOrCopy(file: Path) : Unit = {
      val newTarget = target / FileSupport.toDirectory(file.name)
      if (file.isDirectory) {
        recursiveCopy(file.toDirectory, newTarget)
      } else {
        file.toFile.copyTo(newTarget)
      }
    }
    source.toDirectory.list.foreach(createOrCopy)
  }

  def toDirectory(name: String) : Directory = {
    new Directory(new java.io.File(name))
  }

  def toFile(name: String) : File = {
    new File(new java.io.File(name))
  }
  
}