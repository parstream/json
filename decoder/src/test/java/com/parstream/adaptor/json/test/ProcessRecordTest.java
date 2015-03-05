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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;

import javax.json.JsonObject;

import org.junit.Test;

import com.parstream.adaptor.json.JsonAdaptor;
import com.parstream.adaptor.json.JsonAdaptorException;
import com.parstream.adaptor.json.test.AdaptorTestUtils.Type;
import com.parstream.driver.ColumnInfo;

public class ProcessRecordTest {

    private static String className = "ProcessRecord";

    /**
     * Test that NULL is returned if NULL JsonObject is supplied
     */
    @Test
    public void testNull() throws Exception {
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Null"), new ColumnInfo[0]);
        assertEquals("null JsonObject", 0, decoder.convertJson(null).size());
    }

    @Test
    public void testSingleNestedObject() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "SingleNestedObject");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("strCol", Type.VARSTRING, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "SingleNestedObject"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { "a1" }, res.get(0));
    }

    /**
     * Test if JSON has keys not defined in configuration file.
     */
    @Test
    public void testUnusedKeys() throws Exception {
        JsonObject myObj1 = AdaptorTestUtils.getJsonObj(className, "UnusedKeys", "data1.json");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "UnusedKeys"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj1);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 120 }, res.get(0));

        JsonObject myObj2 = AdaptorTestUtils.getJsonObj(className, "UnusedKeys", "data2.json");
        res = decoder.convertJson(myObj2);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 122 }, res.get(0));
    }

    /**
     * Test if a ParStream column has no corresponding JSON mapping
     */
    @Test
    public void testNullJsonKey() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "NullJsonKey");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "NullJsonKey"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    @Test
    public void testUnsupportedParstreamBlob() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "SingleNestedObject");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("strCol", Type.BLOB, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "SingleNestedObject"), colInfos);
        try {
            decoder.convertJson(myObj);
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue(expected.getMessage().startsWith("ParStream BLOB column type not supported for decoding"));
        }
    }
}
