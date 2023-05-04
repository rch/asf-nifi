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
package org.apache.nifi.processors.document;

import org.apache.commons.io.IOUtils;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.tika.Tika;
import org.apache.tika.exception.TikaConfigException;
import org.apache.tika.extractor.EmbeddedDocumentExtractor;
import org.apache.tika.metadata.Metadata;
import org.apache.tika.mime.MediaType;
import org.apache.tika.parser.AutoDetectParser;
import org.apache.tika.parser.ParseContext;
import org.apache.tika.parser.Parser;
import org.apache.tika.parser.ocr.TesseractOCRParser;
import org.apache.tika.parser.ocr.TesseractOCRConfig;
import org.apache.tika.parser.pdf.PDFParser;
import org.apache.tika.parser.pdf.PDFParserConfig;
import org.apache.tika.sax.BodyContentHandler;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.apache.nifi.expression.ExpressionLanguageScope.NONE;

@Tags({"extract, document, text"})
@CapabilityDescription("Extract text contents from supported binary document formats using Apache Tika")
public class ExtractDocumentText extends AbstractProcessor {
    private static final String TEXT_PLAIN = "text/plain";

    public static final String FIELD_MAX_TEXT_LENGTH = "MAX_TEXT_LENGTH";

    public static final String FIELD_EXTRACT_INLINE_IMAGES = "EXTRACT_INLINE_IMAGES";

    public static final PropertyDescriptor MAX_TEXT_LENGTH = new PropertyDescriptor.Builder()
            .name(FIELD_MAX_TEXT_LENGTH).displayName("Max Output Text Length")
            .description("The maximum length of text to retrieve. Specify -1 for unlimited length.")
            .required(false)
            .defaultValue("-1")
            .addValidator(StandardValidators.INTEGER_VALIDATOR)
            .expressionLanguageSupported(NONE)
            .build();

    public static final PropertyDescriptor EXTRACT_INLINE_IMAGES = new PropertyDescriptor.Builder()
            .name(FIELD_EXTRACT_INLINE_IMAGES).displayName("Extract Text From Images")
            .description("Include text from inline images using an external OCR service like Tesseract.")
            .required(false)
            .defaultValue("False")
            .allowableValues("True", "False")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .expressionLanguageSupported(NONE)
            .build();

    public static final Relationship REL_ORIGINAL = new Relationship.Builder().name("original")
            .description("Success for original input FlowFiles").build();

    public static final Relationship REL_EXTRACTED = new Relationship.Builder().name("extracted")
            .description("Success for extracted text FlowFiles").build();

    public static final Relationship REL_FAILURE = new Relationship.Builder().name("failure")
            .description("Content extraction failed").build();

    private static final Set<Relationship> RELATIONSHIPS = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(REL_ORIGINAL, REL_EXTRACTED, REL_FAILURE)));

    private String slice(String s, Integer length) {
        if (length == null || length < 1 || s.length() <= length) {
            return s;
        } else {
            return s.substring(0, length);
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(MAX_TEXT_LENGTH);
        properties.add(EXTRACT_INLINE_IMAGES);
        return properties;
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        final Integer text_length = context.getProperty(FIELD_MAX_TEXT_LENGTH).evaluateAttributeExpressions(flowFile).asInteger();
        final Boolean ocr_images = context.getProperty(FIELD_EXTRACT_INLINE_IMAGES).evaluateAttributeExpressions(flowFile).asBoolean();

        FlowFile extracted = session.create(flowFile);
        boolean error = false;
        TesseractOCRConfig ocrConfig = new TesseractOCRConfig();
        ocrConfig.setSkipOcr(false);
        ocrConfig.setEnableImagePreprocessing(true);
        ocrConfig.setLanguage("eng");
        PDFParser pdfParser = new PDFParser();
        PDFParserConfig pdfConfig = new PDFParserConfig();
        // Setting values on both parser and config may be unnecessary.
        // TODO: Try applying settings on parser or config only.
        if (ocr_images == true) {
            pdfParser.setExtractInlineImages(true);
            pdfParser.setExtractUniqueInlineImagesOnly(false);
            pdfConfig.setExtractInlineImages(true);
            pdfConfig.setExtractUniqueInlineImagesOnly(false);
        }
        AutoDetectParser parser = new AutoDetectParser();
        ParseContext ctx = new ParseContext();
        ctx.set(TesseractOCRConfig.class, ocrConfig);
        ctx.set(PDFParser.class, pdfParser);
        ctx.set(Parser.class, parser);
        ctx.set(AutoDetectParser.class, parser);
        ctx.set(PDFParserConfig.class, pdfConfig);
        OutputStream os = session.write(extracted);
        BodyContentHandler handler = new BodyContentHandler(os);
        Metadata metadata = new Metadata();
        try (InputStream is = session.read(flowFile);
            OutputStreamWriter writer = new OutputStreamWriter(os)) {
            parser.parse(is, handler, metadata, ctx);
            IOUtils.copy(new StringReader(slice(handler.toString(), text_length)), writer);
        } catch (final Throwable t) {
            error = true;
            getLogger().error("Extraction Failed {}", flowFile, t);
            session.remove(extracted);
            session.transfer(flowFile, REL_FAILURE);
        } finally {
            if (!error) {
                final Map<String, String> attributes = new HashMap<>();
                attributes.put(CoreAttributes.MIME_TYPE.key(), TEXT_PLAIN);
                extracted = session.putAllAttributes(extracted, attributes);
                session.transfer(extracted, REL_EXTRACTED);
                session.transfer(flowFile, REL_ORIGINAL);
            }
        }
    }
}
