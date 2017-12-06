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

import java.awt.Rectangle;
import java.awt.image.BufferedImage;
import java.awt.image.RenderedImage;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.imageio.ImageIO;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.ProcessorInitializationContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.pdfbox.cos.COSName;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDResources;
import org.apache.pdfbox.pdmodel.graphics.PDXObject;
import org.apache.pdfbox.pdmodel.graphics.image.PDImageXObject;
import org.apache.pdfbox.text.PDFTextStripperByArea;



@Tags({"PDF, pdfbox,extract pdf"})
@CapabilityDescription("Provide a description")
@SeeAlso({})
@ReadsAttributes({@ReadsAttribute(attribute="", description="")})
@WritesAttributes({@WritesAttribute(attribute="", description="")})
public class ExtractPDF extends AbstractProcessor {

    public static final PropertyDescriptor CONTENT_TO_EXTRACT = new PropertyDescriptor
            .Builder().name("CONTENT_TO_EXTRACT")
            .displayName("Content to Extract?")
            .description("Extract image or text from the pdf")
            .required(true)
            .allowableValues("IMAGE","TEXT")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor PAGES_TO_EXTRACT = new PropertyDescriptor
            .Builder().name("PAGES_TO_EXTRACT")
            .displayName("Pages to extract from")
            .description("Pages to extract data from")
            .required(true)
            .defaultValue("ALL")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    public static final PropertyDescriptor IMAGE_LOCATION = new PropertyDescriptor
            .Builder().name("IMAGE_LOCATION")
            .displayName("Add Image location to attribute")
            .description("Extract the location of image as attribute")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build(); 
    
    public static final PropertyDescriptor IMAGE_SIZE = new PropertyDescriptor
            .Builder().name("IMAGE_SIZE")
            .displayName("Add Image size to attribute")
            .description("Extract the size of image as attribute")
            .required(true)
            .allowableValues("true","false")
            .defaultValue("false")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build(); 
    public static final PropertyDescriptor IMAGE_TYPE = new PropertyDescriptor
            .Builder().name("IMAGE_TYPE")
            .displayName("set image type as")
            .description("type the extract image is saved as")
            .required(true)
            .allowableValues("tiff","png")
            .defaultValue("tiff")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();
    
    public static final Relationship ORIGINAL = new Relationship.Builder()
            .name("ORIGINAL")
            .description("Original relationship")
            .build();
    
    public static final Relationship FAILURE = new Relationship.Builder()
            .name("FAILURE")
            .description("Original relationship")
            .build();
    

    private List<PropertyDescriptor> descriptors;

    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<PropertyDescriptor>();
        descriptors.add(CONTENT_TO_EXTRACT);
        descriptors.add(PAGES_TO_EXTRACT);
        descriptors.add(IMAGE_LOCATION);
        descriptors.add(IMAGE_SIZE);
        descriptors.add(IMAGE_TYPE);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        relationships.add(ORIGINAL);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @OnScheduled
    public void onScheduled(final ProcessContext context) {

    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if ( flowFile == null ) {
            return;
        }
        
        final ComponentLog logger = this.getLogger();
        final String contentToExtract = context.getProperty(CONTENT_TO_EXTRACT).getValue();
        final Boolean imageLocation = context.getProperty(IMAGE_LOCATION).asBoolean();
        final Boolean imageSize=context.getProperty(IMAGE_SIZE).asBoolean();
        final String imageType= context.getProperty(IMAGE_TYPE).getValue();
        final List<FlowFile> ffList = new ArrayList<FlowFile>(); 
        try{
        
        ProcessStreamCallback callback = new ProcessStreamCallback();
        session.read(flowFile, callback);
        PDDocument document =callback.document;
        int pageNum=0;
    	for(PDPage page:document.getPages()){
    		pageNum++;
    		if("IMAGE".equals(contentToExtract)){
    			int imageNum=0;
            	//System.out.println(pageTree.getCount());
                PDResources pdResources = page.getResources();
                for(COSName cosName: pdResources.getXObjectNames()){
                if (cosName != null) {
                	    imageNum++;
                	//if(pdResources.isImageXObject(cosName)){
                		PDXObject image =pdResources.getXObject(cosName);
                		if(image instanceof PDImageXObject){
                			PDImageXObject pdImage = (PDImageXObject)image;
                		BufferedImage imageStream = pdImage.getImage();
                        //imageStream=createFlipped(imageStream);
                		FlowFile imgFF = session.create(flowFile);
                		imgFF = session.putAttribute(imgFF, "page", pageNum+"");
                		imgFF = session.putAttribute(imgFF, "image", imageNum+"");
                		imgFF = session.write(imgFF, new OutputStreamCallback() {

							@Override
							public void process(OutputStream out) throws IOException {
								// TODO Auto-generated method stub
								ImageIO.write((RenderedImage)imageStream,imageType, ImageIO.createImageOutputStream(out));
							}
                			
                		});
                		
                		ffList.add(imgFF);
                		}
                }
                }
    		}else{
    			PDFTextStripperByArea stripper = new PDFTextStripperByArea();
                boolean found=true;
                stripper.setSortByPosition( true );
                Rectangle rect = new Rectangle(0,0,800,600);
                stripper.addRegion( "class1", rect );
                stripper.extractRegions( page );
                String propDetails=stripper.getTextForRegion( "class1" );
                FlowFile txtFF=session.create(flowFile);
                txtFF=session.putAttribute(txtFF, "page", pageNum+"");
                txtFF=session.write(txtFF,  new OutputStreamCallback() {
					@Override
					public void process(OutputStream out) throws IOException {
						out.write(propDetails.getBytes());
					}
                	
                });
                ffList.add(txtFF);
    		}
    	}
    	session.transfer(ffList,SUCCESS);
    	session.transfer(flowFile,ORIGINAL);
    	session.commit();
        }catch(Exception e){
        	logger.error(e.getMessage());
        	session.remove(ffList);
        	session.transfer(flowFile,FAILURE);
        	session.commit();
        }
    }
    
    static class ProcessStreamCallback implements InputStreamCallback {

    	PDDocument document;
    public ProcessStreamCallback(){
    	
    }
	@Override
	public void process(InputStream in) throws IOException {
		// TODO Auto-generated method stub
		this.document=PDDocument.load(in);
	}
    
    }
}
