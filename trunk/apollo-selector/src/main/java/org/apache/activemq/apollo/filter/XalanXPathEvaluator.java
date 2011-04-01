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

package org.apache.activemq.apollo.filter;

import java.io.StringReader;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

import org.fusesource.hawtbuf.Buffer;
import org.fusesource.hawtbuf.BufferInputStream;
import org.apache.xpath.CachedXPathAPI;
import org.apache.xpath.objects.XObject;
import org.w3c.dom.Document;
import org.w3c.dom.traversal.NodeIterator;
import org.xml.sax.InputSource;


public class XalanXPathEvaluator implements XPathExpression.XPathEvaluator {

    private final String xpath;

    public XalanXPathEvaluator(String xpath) {
        this.xpath = xpath;
    }

    public boolean evaluate(Filterable m) throws FilterException {
        String stringBody = m.getBodyAs(String.class);
        if (stringBody!=null) {
            return evaluate(stringBody);
        } 
        
        Buffer bufferBody = m.getBodyAs(Buffer.class);
        if (bufferBody!=null) {
            return evaluate(bufferBody);
        } 
        return false;
    }

    private boolean evaluate(Buffer data) {
        try {

            InputSource inputSource = new InputSource(new BufferInputStream(data));

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder dbuilder = factory.newDocumentBuilder();
            Document doc = dbuilder.parse(inputSource);
            
            CachedXPathAPI cachedXPathAPI = new CachedXPathAPI();
            XObject result = cachedXPathAPI.eval(doc, xpath);
            if (result.bool())
            	return true;
            else {
            	NodeIterator iterator = cachedXPathAPI.selectNodeIterator(doc, xpath);
            	return (iterator.nextNode() != null);
            }  

        } catch (Throwable e) {
            return false;
        }
    }

    private boolean evaluate(String text) {
        try {
            InputSource inputSource = new InputSource(new StringReader(text));

            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setNamespaceAware(true);
            DocumentBuilder dbuilder = factory.newDocumentBuilder();
            Document doc = dbuilder.parse(inputSource);

            //An XPath expression could return a true or false value instead of a node.
            //eval() is a better way to determine the boolean value of the exp.
            //For compliance with legacy behavior where selecting an empty node returns true,
            //selectNodeIterator is attempted in case of a failure.
            
            CachedXPathAPI cachedXPathAPI = new CachedXPathAPI();
            XObject result = cachedXPathAPI.eval(doc, xpath);
            if (result.bool())
            	return true;
            else {
            	NodeIterator iterator = cachedXPathAPI.selectNodeIterator(doc, xpath);
            	return (iterator.nextNode() != null);
            }    	
            
        } catch (Throwable e) {
            return false;
        }
    }
}
