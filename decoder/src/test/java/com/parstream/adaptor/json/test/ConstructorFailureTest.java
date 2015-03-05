/**
 * Copyright 2015 ParStream GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.parstream.adaptor.json.test;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.junit.Test;

import com.parstream.adaptor.json.JsonAdaptor;
import com.parstream.adaptor.json.JsonAdaptorException;
import com.parstream.driver.ColumnInfo;

public class ConstructorFailureTest {

    private static String className = "Constructor";

    /**
     * Test NULL configuration file parameter.
     */
    @Test
    public void testNullConfigFilePath() throws Exception {
        try {
            File mappingFile = null;
            new JsonAdaptor(mappingFile, null);
            fail("new instance of ParstreamJsonDecoder with null config file path should throw exception");
        } catch (AssertionError expected) {
            assertTrue(expected.getMessage().equals("config file path must not be null"));
        }
    }

    /**
     * Test configuration file does not exist.
     */
    @Test
    public void testNonExistantConfigFilePath() throws Exception {
        try {
            new JsonAdaptor(new File("shskjghkdfhgkshf"), new ColumnInfo[0]);
            fail("new instance of ParstreamJsonDecoder with non-existant config file path should throw exception");
        } catch (FileNotFoundException expected) {
        }
    }

    @Test
    public void testNoMappingsInConfigFile() throws Exception {
        try {
            new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "NoMappingsInConfigFile"), new ColumnInfo[0]);
            fail("new instance of ParstreamJsonDecoder with no column mappings should throw exception");
        } catch (JsonAdaptorException expected) {
            assertTrue(expected.getMessage().equals("no column mappings specified"));
        }
    }

    /**
     * Test NULL ColumnInfo parameter.
     */
    @Test
    public void testValidConfigNullColumnInfo() throws Exception {
        try {
            new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "ValidConfigNullColumnInfo"), null);
            fail("new instance of ParstreamJsonDecoder with no ColumnInfo should throw exception");
        } catch (AssertionError expected) {
            assertTrue(expected.getMessage().equals("ColumnInfo must not be null"));
        }
    }

    /**
     * Test with a valid config file, but empty ColumnInfo
     */
    @Test
    public void testValidConfigEmptyColumnInfo() throws Exception, JsonAdaptorException {
        new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "ValidConfigEmptyColumnInfo"), new ColumnInfo[0]);
    }

    /* Using InputStream for mapping file */

    @Test
    public void testNullConfigFilePathWithInputStream() throws Exception {
        try {
            InputStream mappingStream = null;
            new JsonAdaptor(mappingStream, null);
            fail("new instance of ParstreamJsonDecoder with null config file path should throw exception");
        } catch (AssertionError expected) {
            assertTrue(expected.getMessage().equals("input stream must not be null"));
        }
    }

    @Test
    public void testValidConfigNullColumnInfoWithInputStream() throws Exception {
        try {
            new JsonAdaptor(
                    ConstructorFailureTest.class.getResourceAsStream("/Constructor/ValidConfigNullColumnInfo/json.ini"),
                    null);
            fail("new instance of ParstreamJsonDecoder with no ColumnInfo should throw exception");
        } catch (AssertionError expected) {
            assertTrue(expected.getMessage().equals("ColumnInfo must not be null"));
        }
    }

    @Test
    public void testNoMappingsInConfigFileWithInputStream() throws Exception {
        try {
            new JsonAdaptor(
                    ConstructorFailureTest.class.getResourceAsStream("/Constructor/NoMappingsInConfigFile/json.ini"),
                    new ColumnInfo[0]);
            fail("new instance of ParstreamJsonDecoder with no column mappings should throw exception");
        } catch (JsonAdaptorException expected) {
            assertTrue(expected.getMessage().equals("no column mappings specified"));
        }
    }
}
