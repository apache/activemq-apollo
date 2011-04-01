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

package org.apache.activemq.apollo.web

import org.fusesource.scalate.support.DefaultTemplatePackage
import org.fusesource.scalate.{Binding, TemplateSource}

/**
 * Defines the template package of reusable imports, attributes and methods across templates
 *
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class ScalatePackage extends DefaultTemplatePackage {
  override def header(source: TemplateSource, bindings: List[Binding]) = super.header(source,bindings)+"""
    // common imports go here
  """
}
