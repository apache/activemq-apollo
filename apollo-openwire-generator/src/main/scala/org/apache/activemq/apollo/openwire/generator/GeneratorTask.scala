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

package org.apache.activemq.apollo.openwire.generator

import java.io.File
import org.apache.tools.ant.BuildException
import org.apache.tools.ant.Project
import org.apache.tools.ant.Task
import org.codehaus.jam.JamService
import org.codehaus.jam.JamServiceFactory
import org.codehaus.jam.JamServiceParams
import org.apache.activemq.openwire.tool.{JavaTestsGenerator, JavaMarshallingGenerator}
import reflect.BeanProperty


/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object GeneratorTask {
  def main(args: Array[String]): Unit = {
    var project = new Project
    project.init
    var generator = new GeneratorTask
    generator.setProject(project)
    if (args.length > 0) {
      generator.version = Integer.parseInt(args(0))
    }
    if (args.length > 1) {
      generator.sourceDir = new File(args(1))
    }
    if (args.length > 2) {
      generator.targetDir = new File(args(2))
    }
    if (args.length > 3) {
      generator.packagePrefix = args(3)
    }
    if (args.length > 4) {
      generator.commandPackage = args(4)
    }
    generator.execute
  }
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class GeneratorTask extends Task {

  @BeanProperty
  var first = 1

  @BeanProperty
  var version = 1

  @BeanProperty
  var sourceDir = new File("./src/main/scala")

  @BeanProperty
  var targetDir = new File("./src/main/scala")

  @BeanProperty
  var commandPackage:String = null

  @BeanProperty
  var packagePrefix:String = null


  override def execute: Unit = {
    try {

      println("Parsing source files in: " + sourceDir)
      var jamServiceFactory = JamServiceFactory.getInstance
      var params = jamServiceFactory.createServiceParams
      var dirs = Array(sourceDir)
      params.includeSourcePattern(dirs, "**/*.java")

      var jam = jamServiceFactory.createService(params)

      for( i <- first.to(version)) {
        println("======================================================")
        println(" Generating Marshallers for OpenWire version: " + i)
        println("======================================================")
        var script = new ApolloMarshallingGenerator
        script.setJam(jam)
        script.setTargetDir(targetDir.getCanonicalPath)
        script.setOpenwireVersion(i)
        if( getPackagePrefix()!=null ) {
          script.setPackagePrefix(getPackagePrefix())
        }
        if( getCommandPackage()!=null ) {
          script.setCommandPackage(getCommandPackage())
        }
        script.run
      }

    } catch {
      case e: Exception => throw new BuildException(e)
    }
  }

}

