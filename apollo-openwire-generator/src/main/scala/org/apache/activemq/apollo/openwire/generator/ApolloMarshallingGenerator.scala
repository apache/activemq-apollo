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
import java.io.FileWriter
import java.io.PrintWriter
import java.util.ArrayList
import java.util.Collections
import java.util.Comparator
import java.util.List

import org.apache.activemq.openwire.tool.MultiSourceGenerator
import collection.JavaConversions._
import org.apache.tools.ant.Project
import org.apache.tools.ant.taskdefs.FixCRLF
import org.apache.tools.ant.taskdefs.FixCRLF.CrLf
import org.codehaus.jam._
import scala.beans.BeanProperty

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ApolloMarshallingGenerator extends MultiSourceGenerator {

  protected var concreteClasses: List[JClass] = new ArrayList[JClass]
  protected var factoryFile: File = null
  protected var factoryFileName: String = "MarshallerFactory"
  protected var indent: String = "    "
  protected var targetDir: String = "src/main/java"

  @BeanProperty
  var commandPackage = "org.apache.activemq.apollo.openwire.command"

  @BeanProperty
  var packagePrefix = "org.apache.activemq.apollo.openwire.codec"

  def packagePrefixPath = packagePrefix.replace('.', '/');

  override def run: AnyRef = {
    if (destDir == null) {
      destDir = new File(targetDir + "/"+packagePrefixPath+"/v"+getOpenwireVersion)
    }
    var answer: AnyRef = super.run
    processFactory
    return answer
  }


  def includeInThisVersion(annotation: JAnnotation) = {
    Option(annotation.getValue("version")).map(_.asInt() <= getOpenwireVersion).getOrElse(true)
  }

  override protected def isValidClass(jclass: JClass): Boolean = {
    val annotation = jclass.getAnnotation("openwire:marshaller")
    if (annotation == null) {
      return false
    }
    if(!includeInThisVersion(annotation)) {
      return false
    }
    return !manuallyMaintainedClasses.contains(jclass.getSimpleName)
  }

  override def isValidProperty(it: JProperty): Boolean = {
    val getter = it.getGetter
    if( getter == null || it.getSetter == null || getter.isStatic  )
      return false
    val annotation = getter.getAnnotation("openwire:property")
    if (annotation == null) {
      return false
    }
    if(!includeInThisVersion(annotation)) {
      return false
    }
    return true
  }

  protected def generateFile(out: PrintWriter): Unit = {
    generateLicence(out)
    out.println("")
    out.println("package "+packagePrefix +".v"+ getOpenwireVersion + ";")
    out.println("")
    out.println("import org.fusesource.hawtbuf.DataByteArrayInputStream;")
    out.println("import org.fusesource.hawtbuf.DataByteArrayOutputStream;")
    out.println("import java.io.IOException;")
    out.println("")
    out.println("import "+packagePrefix+".*;")
    out.println("import "+commandPackage+".*;")

    out.println("")
    out.println("")

    getJclass.getImportedPackages.foreach { pkg =>
      pkg.getClasses.foreach { clazz =>
        out.println("import " + clazz.getQualifiedName + ";")
      }
    }

    out.println("")
    out.println("/**")
    out.println(" * Marshalling code for Open Wire Format for " + getClassName() + "")
    out.println(" *")
    out.println(" *")
    out.println(" * NOTE!: This file is auto generated - do not modify!");
    out.println(" *        Modify the 'apollo-openwire-generator' module instead.");
    out.println(" *")
    out.println(" */")
    out.println("public " + getAbstractClassText + "class " + getClassName() + " extends " + getBaseClass + " {")
    out.println("")
    if (!isAbstractClass) {
      out.println("    /**")
      out.println("     * Return the type of Data Structure we marshal")
      out.println("     * @return short representation of the type data structure")
      out.println("     */")
      out.println("    public byte getDataStructureType() {")
      out.println("        return " + getJclass.getSimpleName + ".DATA_STRUCTURE_TYPE;")
      out.println("    }")
      out.println("    ")
      out.println("    /**")
      out.println("     * @return a new object instance")
      out.println("     */")
      out.println("    public DataStructure createObject() {")
      out.println("        return new " + getJclass.getSimpleName + "();")
      out.println("    }")
      out.println("")
    }
    out.println("    /**")
    out.println("     * Un-marshal an object instance from the data input stream")
    out.println("     *")
    out.println("     * @param o the object to un-marshal")
    out.println("     * @param dataIn the data input stream to build the object from")
    out.println("     * @throws IOException")
    out.println("     */")
    out.println("    public void tightUnmarshal(OpenWireFormat wireFormat, Object o, DataByteArrayInputStream dataIn, BooleanStream bs) throws IOException {")
    out.println("        super.tightUnmarshal(wireFormat, o, dataIn, bs);")
    if (!getProperties.isEmpty) {
      out.println("")
      out.println("        " + getJclass.getSimpleName + " info = (" + getJclass.getSimpleName + ")o;")
    }
    if (isMarshallerAware) {
      out.println("")
      out.println("        info.beforeUnmarshall(wireFormat);")
      out.println("        ")
    }
    generateTightUnmarshalBody(out)
    if (isMarshallerAware) {
      out.println("")
      out.println("        info.afterUnmarshall(wireFormat);")
    }
    out.println("")
    out.println("    }")
    out.println("")
    out.println("")
    out.println("    /**")
    out.println("     * Write the booleans that this object uses to a BooleanStream")
    out.println("     */")
    out.println("    public int tightMarshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {")
    if (!getProperties.isEmpty) {
      out.println("")
      out.println("        " + getJclass.getSimpleName + " info = (" + getJclass.getSimpleName + ")o;")
    }
    if (isMarshallerAware) {
      out.println("")
      out.println("        info.beforeMarshall(wireFormat);")
    }
    out.println("")
    out.println("        int rc = super.tightMarshal1(wireFormat, o, bs);")
    var baseSize: Int = generateTightMarshal1Body(out)
    out.println("")
    out.println("        return rc + " + baseSize + ";")
    out.println("    }")
    out.println("")
    out.println("    /**")
    out.println("     * Write a object instance to data output stream")
    out.println("     *")
    out.println("     * @param o the instance to be marshaled")
    out.println("     * @param dataOut the output stream")
    out.println("     * @throws IOException thrown if an error occurs")
    out.println("     */")
    out.println("    public void tightMarshal2(OpenWireFormat wireFormat, Object o, DataByteArrayOutputStream dataOut, BooleanStream bs) throws IOException {")
    out.println("        super.tightMarshal2(wireFormat, o, dataOut, bs);")
    if (!getProperties.isEmpty) {
      out.println("")
      out.println("        " + getJclass.getSimpleName + " info = (" + getJclass.getSimpleName + ")o;")
    }
    generateTightMarshal2Body(out)
    if (isMarshallerAware) {
      out.println("")
      out.println("        info.afterMarshall(wireFormat);")
    }
    out.println("")
    out.println("    }")
    out.println("")
    out.println("    /**")
    out.println("     * Un-marshal an object instance from the data input stream")
    out.println("     *")
    out.println("     * @param o the object to un-marshal")
    out.println("     * @param dataIn the data input stream to build the object from")
    out.println("     * @throws IOException")
    out.println("     */")
    out.println("    public void looseUnmarshal(OpenWireFormat wireFormat, Object o, DataByteArrayInputStream dataIn) throws IOException {")
    out.println("        super.looseUnmarshal(wireFormat, o, dataIn);")
    if (!getProperties.isEmpty) {
      out.println("")
      out.println("        " + getJclass.getSimpleName + " info = (" + getJclass.getSimpleName + ")o;")
    }
    if (isMarshallerAware) {
      out.println("")
      out.println("        info.beforeUnmarshall(wireFormat);")
      out.println("        ")
    }
    generateLooseUnmarshalBody(out)
    if (isMarshallerAware) {
      out.println("")
      out.println("        info.afterUnmarshall(wireFormat);")
    }
    out.println("")
    out.println("    }")
    out.println("")
    out.println("")
    out.println("    /**")
    out.println("     * Write the booleans that this object uses to a BooleanStream")
    out.println("     */")
    out.println("    public void looseMarshal(OpenWireFormat wireFormat, Object o, DataByteArrayOutputStream dataOut) throws IOException {")
    if (!getProperties.isEmpty) {
      out.println("")
      out.println("        " + getJclass.getSimpleName + " info = (" + getJclass.getSimpleName + ")o;")
    }
    if (isMarshallerAware) {
      out.println("")
      out.println("        info.beforeMarshall(wireFormat);")
    }
    out.println("")
    out.println("        super.looseMarshal(wireFormat, o, dataOut);")
    generateLooseMarshalBody(out)
    out.println("")
    out.println("    }")
    out.println("}")
  }

  private def generateLicence(out: PrintWriter): Unit = {
    out.println("/**")
    out.println(" * Licensed to the Apache Software Foundation (ASF) under one or more")
    out.println(" * contributor license agreements.  See the NOTICE file distributed with")
    out.println(" * this work for additional information regarding copyright ownership.")
    out.println(" * The ASF licenses this file to You under the Apache License, Version 2.0")
    out.println(" * (the \"License\"); you may not use this file except in compliance with")
    out.println(" * the License.  You may obtain a copy of the License at")
    out.println(" *")
    out.println(" *      http://www.apache.org/licenses/LICENSE-2.0")
    out.println(" *")
    out.println(" * Unless required by applicable law or agreed to in writing, software")
    out.println(" * distributed under the License is distributed on an \"AS IS\" BASIS,")
    out.println(" * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.")
    out.println(" * See the License for the specific language governing permissions and")
    out.println(" * limitations under the License.")
    out.println(" */")
  }

  protected def processFactory: Unit = {
    if (factoryFile == null) {
      factoryFile = new File(destDir, factoryFileName + filePostFix)
    }
    var out: PrintWriter = null
    try {
      out = new PrintWriter(new FileWriter(factoryFile))
      generateFactory(out)
    } catch {
      case e: Exception => {
        throw new RuntimeException(e)
      }
    } finally {
      if (out != null) {
        out.close
      }
    }
  }

  protected def generateFactory(out: PrintWriter): Unit = {
    generateLicence(out)
    out.println("")
    out.println("package "+packagePrefix+".v" + getOpenwireVersion + ";")
    out.println("")
    out.println("import "+packagePrefix+".DataStreamMarshaller;")
    out.println("import "+packagePrefix+".OpenWireFormat;")
    out.println("")
    out.println("/**")
    out.println(" * MarshallerFactory for Open Wire Format.")
    out.println(" *")
    out.println(" *")
    out.println(" * NOTE!: This file is auto generated - do not modify!");
    out.println(" *        Modify the 'apollo-openwire-generator' module instead.");
    out.println(" *")
    out.println(" * ")
    out.println(" */")
    out.println("public class MarshallerFactory {")
    out.println("")
    out.println("    /**")
    out.println("     * Creates a Map of command type -> Marshallers")
    out.println("     */")
    out.println("    static final private DataStreamMarshaller marshaller[] = new DataStreamMarshaller[256];")
    out.println("    static {")
    out.println("")

    getConcreteClasses.sortBy( _.getSimpleName ).foreach {jclass =>
      out.println("        add(new " + jclass.getSimpleName + "Marshaller());")
    }

    out.println("")
    out.println("    }")
    out.println("")
    out.println("    static private void add(DataStreamMarshaller dsm) {")
    out.println("        marshaller[dsm.getDataStructureType()] = dsm;")
    out.println("    }")
    out.println("    ")
    out.println("    static public DataStreamMarshaller[] createMarshallerMap(OpenWireFormat wireFormat) {")
    out.println("        return marshaller;")
    out.println("    }")
    out.println("}")
  }

  protected override def processClass(jclass: JClass): Unit = {
    super.processClass(jclass)

    // Fix CRLF issues
    var project: Project = new Project
    project.init
    var fixCRLF: FixCRLF = new FixCRLF
    fixCRLF.setProject(project)
    fixCRLF.setSrcdir(destFile.getParentFile)
    fixCRLF.setIncludes(destFile.getName)
    var kind = new FixCRLF.CrLf()
    kind.setValue("unix")
    fixCRLF.setEol(kind)
    fixCRLF.execute

    if (!jclass.isAbstract) {
      concreteClasses.add(jclass)
    }
  }

  protected override def getClassName(jclass: JClass): String = {
    return super.getClassName(jclass) + "Marshaller"
  }

  protected override def getBaseClassName(jclass: JClass): String = {
    var answer: String = "BaseDataStreamMarshaller"
    var superclass: JClass = jclass.getSuperclass
    if (superclass != null) {
      var superName: String = superclass.getSimpleName
      if (!(superName == "Object") && !(superName == "JNDIBaseStorable") && !(superName == "DataStructureSupport")) {
        answer = superName + "Marshaller"
      }
    }
    return answer
  }

  protected override def initialiseManuallyMaintainedClasses: Unit = {
  }

  protected def generateTightUnmarshalBody(out: PrintWriter): Unit = {
    getProperties.foreach {property =>
      var annotation: JAnnotation = property.getAnnotation("openwire:property")
      var size: JAnnotationValue = annotation.getValue("size")
      var propertyType: JClass = property.getType
      var propertyTypeName: String = propertyType.getSimpleName
      if (propertyType.isArrayType && !(propertyTypeName == "byte[]")) {
        generateTightUnmarshalBodyForArrayProperty(out, property, size)
      } else {
        generateTightUnmarshalBodyForProperty(out, property, size)
      }
    }
  }

  protected def generateTightUnmarshalBodyForProperty(out: PrintWriter, property: JProperty, size: JAnnotationValue): Unit = {
    var setter = property.getSetter.getSimpleName
    var property_type = property.getType.getSimpleName
    if (property_type == "boolean") {
      out.println("        info." + setter + "(bs.readBoolean());")
    } else if (property_type == "byte") {
      out.println("        info." + setter + "(dataIn.readByte());")
    } else if (property_type == "char") {
      out.println("        info." + setter + "(dataIn.readChar());")
    } else if (property_type == "short") {
      out.println("        info." + setter + "(dataIn.readShort());")
    } else if (property_type == "int") {
      out.println("        info." + setter + "(dataIn.readInt());")
    } else if (property_type == "long") {
      out.println("        info." + setter + "(tightUnmarshalLong(wireFormat, dataIn, bs));")
    } else if (property_type == "UTF8Buffer") {
      out.println("        info." + setter + "(tightUnmarshalString(dataIn, bs));")
    } else if (property_type == "String") {
      out.println("        info." + setter + "(tightUnmarshalString(dataIn, bs));")
    } else if (property_type == "byte[]") {
      if (size != null) {
        out.println("        info." + setter + "(tightUnmarshalConstByteArray(dataIn, bs, " + size.asInt + "));")
      } else {
        out.println("        info." + setter + "(tightUnmarshalByteArray(dataIn, bs));")
      }
    } else if (property_type == "Buffer") {
      out.println("        info." + setter + "(tightUnmarshalBuffer(dataIn, bs));")
    } else if (isThrowable(property.getType)) {
      out.println("        info." + setter + "((" + property.getType.getQualifiedName + ")tightUnmarsalThrowable(wireFormat, dataIn, bs));")
    } else if (isCachedProperty(property)) {
      out.println("        info." + setter + "((" + property.getType.getQualifiedName + ")tightUnmarsalCachedObject(wireFormat, dataIn, bs));")
    } else {
      out.println("        info." + setter + "((" + property.getType.getQualifiedName + ")tightUnmarsalNestedObject(wireFormat, dataIn, bs));")
    }
  }

  protected def generateTightUnmarshalBodyForArrayProperty(out: PrintWriter, property: JProperty, size: JAnnotationValue): Unit = {
    var propertyType: JClass = property.getType
    var arrayType: String = propertyType.getArrayComponentType.getQualifiedName
    var setter: String = property.getSetter.getSimpleName
    out.println
    if (size != null) {
      out.println("        {")
      out.println("            " + arrayType + " value[] = new " + arrayType + "[" + size.asInt + "];")
      out.println("            " + "for( int i=0; i < " + size.asInt + "; i++ ) {")
      out.println("                value[i] = (" + arrayType + ") tightUnmarsalNestedObject(wireFormat,dataIn, bs);")
      out.println("            }")
      out.println("            info." + setter + "(value);")
      out.println("        }")
    } else {
      out.println("        if (bs.readBoolean()) {")
      out.println("            short size = dataIn.readShort();")
      out.println("            " + arrayType + " value[] = new " + arrayType + "[size];")
      out.println("            for( int i=0; i < size; i++ ) {")
      out.println("                value[i] = (" + arrayType + ") tightUnmarsalNestedObject(wireFormat,dataIn, bs);")
      out.println("            }")
      out.println("            info." + setter + "(value);")
      out.println("        }")
      out.println("        else {")
      out.println("            info." + setter + "(null);")
      out.println("        }")
    }
  }

  protected def generateTightMarshal1Body(out: PrintWriter): Int = {
    var baseSize: Int = 0
    getProperties.foreach {property =>
      var annotation: JAnnotation = property.getAnnotation("openwire:property")
      var size: JAnnotationValue = annotation.getValue("size")
      var propertyType: JClass = property.getType
      var property_type: String = propertyType.getSimpleName
      var getter: String = "info." + property.getGetter.getSimpleName + "()"
      if (property_type == "boolean") {
        out.println("        bs.writeBoolean(" + getter + ");")
      } else if (property_type == "byte") {
        baseSize += 1
      } else if (property_type == "char") {
        baseSize += 2
      } else if (property_type == "short") {
        baseSize += 2
      } else if (property_type == "int") {
        baseSize += 4
      } else if (property_type == "long") {
        out.println("        rc += tightMarshalLong1(wireFormat, " + getter + ", bs);")
      } else if (property_type == "UTF8Buffer") {
        out.println("        rc += tightMarshalString1(" + getter + ", bs);")
      } else if (property_type == "String") {
        out.println("        rc += tightMarshalString1(" + getter + ", bs);")
      } else if (property_type == "byte[]") {
        if (size == null) {
          out.println("        rc += tightMarshalByteArray1(" + getter + ", bs);")
        } else {
          out.println("        rc += tightMarshalConstByteArray1(" + getter + ", bs, " + size.asInt + ");")
        }
      } else if (property_type == "Buffer") {
        out.println("        rc += tightMarshalBuffer1(" + getter + ", bs);")
      } else if (propertyType.isArrayType) {
        if (size != null) {
          out.println("        rc += tightMarshalObjectArrayConstSize1(wireFormat, " + getter + ", bs, " + size.asInt + ");")
        } else {
          out.println("        rc += tightMarshalObjectArray1(wireFormat, " + getter + ", bs);")
        }
      } else if (isThrowable(propertyType)) {
        out.println("        rc += tightMarshalThrowable1(wireFormat, " + getter + ", bs);")
      } else {
        if (isCachedProperty(property)) {
          out.println("        rc += tightMarshalCachedObject1(wireFormat, (DataStructure)" + getter + ", bs);")
        } else {
          out.println("        rc += tightMarshalNestedObject1(wireFormat, (DataStructure)" + getter + ", bs);")
        }
      }
    }
    return baseSize
  }

  protected def generateTightMarshal2Body(out: PrintWriter): Unit = {
    getProperties.foreach {property =>
      var annotation: JAnnotation = property.getAnnotation("openwire:property")
      var size: JAnnotationValue = annotation.getValue("size")
      var propertyType: JClass = property.getType
      var property_type: String = propertyType.getSimpleName
      var getter: String = "info." + property.getGetter.getSimpleName + "()"
      if (property_type == "boolean") {
        out.println("        bs.readBoolean();")
      } else if (property_type == "byte") {
        out.println("        dataOut.writeByte(" + getter + ");")
      } else if (property_type == "char") {
        out.println("        dataOut.writeChar(" + getter + ");")
      } else if (property_type == "short") {
        out.println("        dataOut.writeShort(" + getter + ");")
      } else if (property_type == "int") {
        out.println("        dataOut.writeInt(" + getter + ");")
      } else if (property_type == "long") {
        out.println("        tightMarshalLong2(wireFormat, " + getter + ", dataOut, bs);")
      } else if (property_type == "UTF8Buffer") {
        out.println("        tightMarshalString2(" + getter + ", dataOut, bs);")
      } else if (property_type == "String") {
        out.println("        tightMarshalString2(" + getter + ", dataOut, bs);")
      } else if (property_type == "byte[]") {
        if (size != null) {
          out.println("        tightMarshalConstByteArray2(" + getter + ", dataOut, bs, " + size.asInt + ");")
        } else {
          out.println("        tightMarshalByteArray2(" + getter + ", dataOut, bs);")
        }
      } else if (property_type == "Buffer") {
        out.println("        tightMarshalBuffer2(" + getter + ", dataOut, bs);")
      } else if (propertyType.isArrayType) {
        if (size != null) {
          out.println("        tightMarshalObjectArrayConstSize2(wireFormat, " + getter + ", dataOut, bs, " + size.asInt + ");")
        } else {
          out.println("        tightMarshalObjectArray2(wireFormat, " + getter + ", dataOut, bs);")
        }
      } else if (isThrowable(propertyType)) {
        out.println("        tightMarshalThrowable2(wireFormat, " + getter + ", dataOut, bs);")
      } else {
        if (isCachedProperty(property)) {
          out.println("        tightMarshalCachedObject2(wireFormat, (DataStructure)" + getter + ", dataOut, bs);")
        } else {
          out.println("        tightMarshalNestedObject2(wireFormat, (DataStructure)" + getter + ", dataOut, bs);")
        }
      }
    }
  }

  protected def generateLooseMarshalBody(out: PrintWriter): Unit = {
    getProperties.foreach {property =>
      var annotation: JAnnotation = property.getAnnotation("openwire:property")
      var size: JAnnotationValue = annotation.getValue("size")
      var propertyType: JClass = property.getType
      var property_type: String = propertyType.getSimpleName
      var getter: String = "info." + property.getGetter.getSimpleName + "()"
      if (property_type == "boolean") {
        out.println("        dataOut.writeBoolean(" + getter + ");")
      } else if (property_type == "byte") {
        out.println("        dataOut.writeByte(" + getter + ");")
      } else if (property_type == "char") {
        out.println("        dataOut.writeChar(" + getter + ");")
      } else if (property_type == "short") {
        out.println("        dataOut.writeShort(" + getter + ");")
      } else if (property_type == "int") {
        out.println("        dataOut.writeInt(" + getter + ");")
      } else if (property_type == "long") {
        out.println("        looseMarshalLong(wireFormat, " + getter + ", dataOut);")
      } else if (property_type == "UTF8Buffer") {
        out.println("        looseMarshalString(" + getter + ", dataOut);")
      } else if (property_type == "String") {
        out.println("        looseMarshalString(" + getter + ", dataOut);")
      } else if (property_type == "byte[]") {
        if (size != null) {
          out.println("        looseMarshalConstByteArray(wireFormat, " + getter + ", dataOut, " + size.asInt + ");")
        } else {
          out.println("        looseMarshalByteArray(wireFormat, " + getter + ", dataOut);")
        }
      } else if (property_type == "Buffer") {
        out.println("        looseMarshalBuffer(wireFormat, " + getter + ", dataOut);")
      } else if (propertyType.isArrayType) {
        if (size != null) {
          out.println("        looseMarshalObjectArrayConstSize(wireFormat, " + getter + ", dataOut, " + size.asInt + ");")
        } else {
          out.println("        looseMarshalObjectArray(wireFormat, " + getter + ", dataOut);")
        }
      } else if (isThrowable(propertyType)) {
        out.println("        looseMarshalThrowable(wireFormat, " + getter + ", dataOut);")
      } else {
        if (isCachedProperty(property)) {
          out.println("        looseMarshalCachedObject(wireFormat, (DataStructure)" + getter + ", dataOut);")
        } else {
          out.println("        looseMarshalNestedObject(wireFormat, (DataStructure)" + getter + ", dataOut);")
        }
      }
    }
  }

  protected def generateLooseUnmarshalBody(out: PrintWriter): Unit = {
    getProperties.foreach {property =>
      var annotation = property.getAnnotation("openwire:property")
      var size = annotation.getValue("size")
      var propertyType = property.getType
      var propertyTypeName: String = propertyType.getSimpleName
      if (propertyType.isArrayType && !(propertyTypeName == "byte[]")) {
        generateLooseUnmarshalBodyForArrayProperty(out, property, size)
      } else {
        generateLooseUnmarshalBodyForProperty(out, property, size)
      }
    }
  }

  protected def generateLooseUnmarshalBodyForProperty(out: PrintWriter, property: JProperty, size: JAnnotationValue): Unit = {
    var setter: String = property.getSetter.getSimpleName
    var property_type: String = property.getType.getSimpleName
    if (property_type == "boolean") {
      out.println("        info." + setter + "(dataIn.readBoolean());")
    } else if (property_type == "byte") {
      out.println("        info." + setter + "(dataIn.readByte());")
    } else if (property_type == "char") {
      out.println("        info." + setter + "(dataIn.readChar());")
    } else if (property_type == "short") {
      out.println("        info." + setter + "(dataIn.readShort());")
    } else if (property_type == "int") {
      out.println("        info." + setter + "(dataIn.readInt());")
    } else if (property_type == "long") {
      out.println("        info." + setter + "(looseUnmarshalLong(wireFormat, dataIn));")
    } else if (property_type == "UTF8Buffer") {
      out.println("        info." + setter + "(looseUnmarshalString(dataIn));")
    } else if (property_type == "String") {
      out.println("        info." + setter + "(looseUnmarshalString(dataIn));")
    } else if (property_type == "byte[]") {
      if (size != null) {
        out.println("        info." + setter + "(looseUnmarshalConstByteArray(dataIn, " + size.asInt + "));")
      } else {
        out.println("        info." + setter + "(looseUnmarshalByteArray(dataIn));")
      }
    } else if (property_type == "Buffer") {
      out.println("        info." + setter + "(looseUnmarshalBuffer(dataIn));")
    } else if (isThrowable(property.getType)) {
      out.println("        info." + setter + "((" + property.getType.getQualifiedName + ")looseUnmarsalThrowable(wireFormat, dataIn));")
    } else if (isCachedProperty(property)) {
      out.println("        info." + setter + "((" + property.getType.getQualifiedName + ")looseUnmarsalCachedObject(wireFormat, dataIn));")
    } else {
      out.println("        info." + setter + "((" + property.getType.getQualifiedName + ")looseUnmarsalNestedObject(wireFormat, dataIn));")
    }
  }

  protected def generateLooseUnmarshalBodyForArrayProperty(out: PrintWriter, property: JProperty, size: JAnnotationValue): Unit = {
    var propertyType: JClass = property.getType
    var arrayType: String = propertyType.getArrayComponentType.getQualifiedName
    var setter: String = property.getSetter.getSimpleName
    out.println
    if (size != null) {
      out.println("        {")
      out.println("            " + arrayType + " value[] = new " + arrayType + "[" + size.asInt + "];")
      out.println("            " + "for( int i=0; i < " + size.asInt + "; i++ ) {")
      out.println("                value[i] = (" + arrayType + ")looseUnmarsalNestedObject(wireFormat,dataIn);")
      out.println("            }")
      out.println("            info." + setter + "(value);")
      out.println("        }")
    } else {
      out.println("        if (dataIn.readBoolean()) {")
      out.println("            short size = dataIn.readShort();")
      out.println("            " + arrayType + " value[] = new " + arrayType + "[size];")
      out.println("            for( int i=0; i < size; i++ ) {")
      out.println("                value[i] = (" + arrayType + ")looseUnmarsalNestedObject(wireFormat,dataIn);")
      out.println("            }")
      out.println("            info." + setter + "(value);")
      out.println("        }")
      out.println("        else {")
      out.println("            info." + setter + "(null);")
      out.println("        }")
    }
  }

  override def isMarshallAware(j: JClass): Boolean = {
    if (filePostFix.endsWith("java")) {
      j.getInterfaces.foreach { x=>
        if (x.getQualifiedName == commandPackage+".MarshallAware") {
          return true
        }
      }
      return false
    } else {
      var simpleName = j.getSimpleName
      return (simpleName == "ActiveMQMessage") || (simpleName == "WireFormatInfo")
    }
  }

  /**
   * Returns whether or not the given annotation has a mandatory flag on it or
   * not
   */
  protected def getMandatoryFlag(annotation: JAnnotation): String = {
    var value: JAnnotationValue = annotation.getValue("mandatory")
    if (value != null) {
      var text: String = value.asString
      if (text != null && text.equalsIgnoreCase("true")) {
        return "true"
      }
    }
    return "false"
  }

  def getConcreteClasses: List[JClass] = {
    return concreteClasses
  }

  def setConcreteClasses(concreteClasses: List[JClass]): Unit = {
    this.concreteClasses = concreteClasses
  }

  def getFactoryFile: File = {
    return factoryFile
  }

  def setFactoryFile(factoryFile: File): Unit = {
    this.factoryFile = factoryFile
  }

  def getFactoryFileName: String = {
    return factoryFileName
  }

  def setFactoryFileName(factoryFileName: String): Unit = {
    this.factoryFileName = factoryFileName
  }

  def getIndent: String = {
    return indent
  }

  def setIndent(indent: String): Unit = {
    this.indent = indent
  }

  def getTargetDir: String = {
    return targetDir
  }

  def setTargetDir(sourceDir: String): Unit = {
    this.targetDir = sourceDir
  }


}

