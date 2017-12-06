/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.processors.pdf;

import java.io.File;
import java.io.IOException;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;


public class TestExtractPDF {

    private TestRunner testRunner;

    @Before
    public void init() {
        testRunner = TestRunners.newTestRunner(ExtractPDF.class);
    }

    @Test
    public void test1PageDocument() throws IOException{
    	
    	File pdf = new File("/Users/knarayanan/ImageOnly.pdf");
    	testRunner.setProperty(ExtractPDF.CONTENT_TO_EXTRACT, "IMAGE");
    	testRunner.enqueue(pdf.toPath());
    	testRunner.run();
    	testRunner.assertTransferCount(ExtractPDF.SUCCESS, 1);
    }
    
    @Test
    public void testMultiPageDocument() throws IOException{
    	
    	File pdf = new File("/Users/knarayanan/listingappraisalsample.pdf");
    	testRunner.setProperty(ExtractPDF.CONTENT_TO_EXTRACT, "IMAGE");
    	testRunner.enqueue(pdf.toPath());
    	testRunner.run();
    	testRunner.assertTransferCount(ExtractPDF.SUCCESS, 18);
    }
    @Test
    public void testMultiPageDocumentText() throws IOException{
    	
    	File pdf = new File("/Users/knarayanan/listingappraisalsample.pdf");
    	testRunner.setProperty(ExtractPDF.CONTENT_TO_EXTRACT, "TEXT");
    	testRunner.enqueue(pdf.toPath());
    	testRunner.run();
    	testRunner.assertTransferCount(ExtractPDF.SUCCESS, 19);
    }

}
