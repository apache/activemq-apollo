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
package org.apache.activemq.apollo.broker.jaxb;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.stream.XMLStreamReader;
import javax.xml.stream.util.StreamReaderDelegate;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 
 * Changes ${system.property} with System.getProperty("system.property") in attribute values
 *
 */
public class PropertiesReader extends StreamReaderDelegate {
	
 Logger LOG = LoggerFactory.getLogger(PropertiesReader.class);
	
	Pattern pattern = Pattern.compile("\\$\\{([a-zA-Z0-9\\.]*)\\}");
	
	public PropertiesReader(XMLStreamReader parent) {
		super(parent);
	}

	public String getAttributeValue(int index) {
		String value = super.getAttributeValue(index);
		String replaced = replaceSystemProperties(value);
		return replaced;
	}
	
	
	public String replaceSystemProperties(String str) {
		int start = 0;
		while (true) {
			Matcher matcher = pattern.matcher(str);
			if (!matcher.find(start)) {
				break;
			}
			String property = System.getProperty(matcher.group(1));
			if (property != null) {
				str = matcher.replaceFirst(property);
			} else {
				if (LOG.isDebugEnabled()) {
					LOG.debug("System property " + matcher.group(1) + " not found");
				}
				start = matcher.end();
			}
		}
		return str;
	}

	
	
}
