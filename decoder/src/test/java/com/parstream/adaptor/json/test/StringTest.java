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

public class StringTest {

    private static String className = "String";

    @Test
    public void testString() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "String");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("name", Type.VARSTRING, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "String"), colInfo);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { "abc" }, res.get(0));
    }

    @Test
    public void testInt() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Int");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("name", Type.VARSTRING, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Int"), colInfo);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { "1" }, res.get(0));
    }

    @Test
    public void testDouble() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Double");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("name", Type.VARSTRING, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Double"), colInfo);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { "12.34" }, res.get(0));
    }

    @Test
    public void testNull() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Null");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("strColumn", Type.VARSTRING, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Null"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }
}
