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
package org.apache.hawtdb.internal.page;

import static org.junit.Assert.assertEquals;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;

import org.apache.hawtdb.internal.page.ExtentInputStream;
import org.apache.hawtdb.internal.page.ExtentOutputStream;
import org.apache.hawtdb.internal.page.PageFile;
import org.apache.hawtdb.internal.page.PageFileFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
public class ExtentTest {

	private PageFileFactory pff;
    private PageFile paged;

    protected PageFileFactory createPageFileFactory() {
	    PageFileFactory rc = new PageFileFactory();
	    rc.setMappingSegementSize(rc.getPageSize()*3);
	    rc.setFile(new File("target/test-data/"+getClass().getName()+".db"));
	    return rc;
	}
    
	@Before
	public void setUp() throws Exception {
        pff = createPageFileFactory();
        pff.getFile().delete();
        pff.open();
        paged = pff.getPageFile();
	}

	@After
	public void tearDown() throws Exception {
	    pff.close();
	}
	
	protected void reload() {
        pff.close();
        pff.open();
        paged = pff.getPageFile();
	}
	

    @Test
	public void testExtentStreams() throws IOException {
        ExtentOutputStream eos = new ExtentOutputStream(paged);
        DataOutputStream os = new DataOutputStream(eos);
        for (int i = 0; i < 10000; i++) {
            os.writeUTF("Test string:" + i);
        }
        os.close();
        int page = eos.getPage();
        
        assertEquals(0, page);

        // Reload the page file.
        reload();

        ExtentInputStream eis = new ExtentInputStream(paged, page);
        DataInputStream is = new DataInputStream(eis);
        for (int i = 0; i < 10000; i++) {
            assertEquals("Test string:" + i, is.readUTF());
        }
        assertEquals(-1, is.read());
        is.close();
    }
}
