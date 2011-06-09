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
import collection.mutable.ArrayBuffer
import java.io.File
import FileSupport._

class FileSupportTest extends FunSuiteSupport with ShouldMatchers {

  test("recursive file copy test") {

    val source_dir = basedir / "target" / "source-dir"
    val target_dir = basedir / "target" / "target-dir"
    List(source_dir, target_dir).foreach(_.recursive_delete)

    (source_dir / "sub-dir" / "some-file").mkdirs

    source_dir.recursive_copy_to(target_dir)

    val listing = target_dir.recursive_list.map(file => file.getCanonicalPath)

    listing should contain( (basedir/"target"/"target-dir"/"sub-dir").getCanonicalPath  )
    listing should contain( (basedir/"target"/"target-dir"/"sub-dir"/"some-file").getCanonicalPath )

  }

  
}