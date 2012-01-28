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
        script.run
      }

    } catch {
      case e: Exception => throw new BuildException(e)
    }
  }

}

