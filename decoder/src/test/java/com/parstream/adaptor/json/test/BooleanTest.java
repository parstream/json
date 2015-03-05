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

import java.io.File;
import java.util.List;

import javax.json.JsonObject;

import org.junit.BeforeClass;
import org.junit.Test;

import com.parstream.adaptor.json.JsonAdaptor;
import com.parstream.adaptor.json.test.AdaptorTestUtils.Type;
import com.parstream.driver.ColumnInfo;

public class BooleanTest {

    private static String className = "Boolean";
    private static File jsonConfig = null;

    @BeforeClass
    public static void booleanTestSetup() throws Exception {
        jsonConfig = AdaptorTestUtils.getIniFile(className);
    }

    @Test
    public void testJsonBooleanBitvector8True() throws Exception {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("boolCol", Type.BITVECTOR8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(jsonConfig, colInfo);

        // true
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanTrue.data");
        List<Object[]> res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 1 }, res.get(0));

        // false
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanFalse.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 0 }, res.get(0));

        // null
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanNull.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    @Test
    public void testJsonBooleanUINT8True() throws Exception {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("boolCol", Type.UINT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(jsonConfig, colInfo);

        // true
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanTrue.data");
        List<Object[]> res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 1 }, res.get(0));

        // false
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanFalse.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 0 }, res.get(0));

        // null
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanNull.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    @Test
    public void testJsonBooleanUINT16True() throws Exception {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("boolCol", Type.UINT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(jsonConfig, colInfo);

        // true
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanTrue.data");
        List<Object[]> res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 1 }, res.get(0));

        // false
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanFalse.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 0 }, res.get(0));

        // null
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanNull.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    @Test
    public void testJsonBooleanUINT32True() throws Exception {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("boolCol", Type.UINT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(jsonConfig, colInfo);

        // true
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanTrue.data");
        List<Object[]> res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 1 }, res.get(0));

        // false
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanFalse.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 0 }, res.get(0));

        // null
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanNull.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    @Test
    public void testJsonBooleanUINT64True() throws Exception {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("boolCol", Type.UINT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(jsonConfig, colInfo);

        // true
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanTrue.data");
        List<Object[]> res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 1 }, res.get(0));

        // false
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanFalse.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 0 }, res.get(0));

        // null
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanNull.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    @Test
    public void testJsonBooleanINT8True() throws Exception {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("boolCol", Type.INT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(jsonConfig, colInfo);

        // true
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanTrue.data");
        List<Object[]> res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 1 }, res.get(0));

        // false
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanFalse.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 0 }, res.get(0));

        // null
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanNull.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    @Test
    public void testJsonBooleanINT16True() throws Exception {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("boolCol", Type.INT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(jsonConfig, colInfo);

        // true
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanTrue.data");
        List<Object[]> res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 1 }, res.get(0));

        // false
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanFalse.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 0 }, res.get(0));

        // null
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanNull.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    @Test
    public void testJsonBooleanINT32True() throws Exception {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("boolCol", Type.INT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(jsonConfig, colInfo);

        // true
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanTrue.data");
        List<Object[]> res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 1 }, res.get(0));

        // false
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanFalse.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 0 }, res.get(0));

        // null
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanNull.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    @Test
    public void testJsonBooleanINT64True() throws Exception {
        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("boolCol", Type.INT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(jsonConfig, colInfo);

        // true
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanTrue.data");
        List<Object[]> res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 1 }, res.get(0));

        // false
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanFalse.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 0 }, res.get(0));

        // null
        myObj = AdaptorTestUtils.getJsonObj(className, null, "jsonBooleanNull.data");
        res = decoder.convertJson(myObj);
        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }
}
