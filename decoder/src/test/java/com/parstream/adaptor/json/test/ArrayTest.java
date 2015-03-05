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

import java.util.List;

import javax.json.JsonObject;

import org.junit.Test;

import com.parstream.adaptor.json.JsonAdaptor;
import com.parstream.adaptor.json.test.AdaptorTestUtils.Type;
import com.parstream.driver.ColumnInfo;

public class ArrayTest {

    private static String className = "Array";

    @Test
    public void testEmptyArrayStrings() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "EmptyArrayStrings");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("abc", AdaptorTestUtils.Type.VARSTRING, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "EmptyArrayStrings"), colInfo);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 0, res.size());
    }

    @Test
    public void testArrayOfTwoStrings() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "ArrayOfTwoStrings");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("abc", AdaptorTestUtils.Type.VARSTRING, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "ArrayOfTwoStrings"), colInfo);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 2, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { "a1" }, res.get(0));
        assertArrayEquals("Object[] in result set", new Object[] { "a2" }, res.get(1));
    }

    @Test
    public void testArrayOfTwoNumbers() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "ArrayOfTwoNumbers");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("name", Type.UINT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "ArrayOfTwoNumbers"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 2, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 1 }, res.get(0));
        assertArrayEquals("Object[] in result set", new Object[] { 2 }, res.get(1));
    }

    @Test
    public void testArrayOfTwoBooleans() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "ArrayOfTwoBooleans");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("name", Type.BITVECTOR8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "ArrayOfTwoBooleans"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 2, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 1 }, res.get(0));
        assertArrayEquals("Object[] in result set", new Object[] { 0 }, res.get(1));
    }

    @Test
    public void testArrayOfTwoNulls() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "ArrayOfTwoNulls");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("name", Type.BITVECTOR8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "ArrayOfTwoNulls"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 2, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(1));
    }

    @Test
    public void testArrayOfArrayOfInts() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "ArrayOfArrayOfInts");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("name", Type.UINT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "ArrayOfArrayOfInts"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 3, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 1 }, res.get(0));
        assertArrayEquals("Object[] in result set", new Object[] { 2 }, res.get(1));
        assertArrayEquals("Object[] in result set", new Object[] { 3 }, res.get(2));
    }

    @Test
    public void testArrayOfTwoObjects() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "ArrayOfTwoObjects");

        ColumnInfo[] colInfos = new ColumnInfo[2];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("id", Type.UINT16, 0, 0);
        colInfos[1] = AdaptorTestUtils.constructColumnInfo("name", Type.VARSTRING, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "ArrayOfTwoObjects"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 2, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 1, "a" }, res.get(0));
        assertArrayEquals("Object[] in result set", new Object[] { 2, "b" }, res.get(1));
    }

    @Test
    public void testArrayOfArraysNoneUsedInConfig() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "ArrayOfArrays");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("name", Type.VARSTRING, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "ArrayOfArrays", "json3.ini"),
                colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { "abc" }, res.get(0));
    }

    @Test
    public void testArrayOfArraysOneUsedInConfig() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "ArrayOfArrays");

        ColumnInfo[] colInfos = new ColumnInfo[2];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("name", Type.VARSTRING, 0, 0);
        colInfos[1] = AdaptorTestUtils.constructColumnInfo("telephone", Type.UINT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "ArrayOfArrays"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 2, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { "abc", 123 }, res.get(0));
        assertArrayEquals("Object[] in result set", new Object[] { "abc", 456 }, res.get(1));
    }

    @Test
    public void testArrayOfArraysAllUsedInConfig() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "ArrayOfArrays");

        ColumnInfo[] colInfos = new ColumnInfo[3];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("name", Type.VARSTRING, 0, 0);
        colInfos[1] = AdaptorTestUtils.constructColumnInfo("telephone", Type.UINT16, 0, 0);
        colInfos[2] = AdaptorTestUtils.constructColumnInfo("address", Type.VARSTRING, 0, 0);

        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "ArrayOfArrays", "json2.ini"),
                colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 4, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { "abc", 123, "a" }, res.get(0));
        assertArrayEquals("Object[] in result set", new Object[] { "abc", 123, "b" }, res.get(1));
        assertArrayEquals("Object[] in result set", new Object[] { "abc", 456, "a" }, res.get(2));
        assertArrayEquals("Object[] in result set", new Object[] { "abc", 456, "b" }, res.get(3));
    }
}
