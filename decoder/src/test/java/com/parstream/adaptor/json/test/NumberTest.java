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

import org.junit.Ignore;
import org.junit.Test;

import com.parstream.adaptor.json.JsonAdaptor;
import com.parstream.adaptor.json.JsonAdaptorException;
import com.parstream.adaptor.json.test.AdaptorTestUtils.Type;
import com.parstream.driver.ColumnInfo;

public class NumberTest {

    private static String className = "Number";

    /** BITVECTOR8 **/

    /**
     * Conversion from String to bitvector8 is not supported. Test should fail.
     */
    @Test
    public void testStringToBITVECTOR8() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "String");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("num", Type.BITVECTOR8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(
                ConstructorFailureTest.class.getResourceAsStream("/Number/String/json.ini"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to bitvector8 conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Shortdate",
                    "Incompatible datatypes for column (num). Database type is BITVECTOR8, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }

    /** UINT8 **/

    @Test
    public void testJsonNumberToUINT8WithinRangeMin() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Zero");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Zero"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 0 }, res.get(0));
    }

    @Test
    public void testJsonNumberToUINT8WithinRange() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Num_120");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Num_120"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 120 }, res.get(0));
    }

    @Test
    public void testJsonNumberToUINT8WithinRangeMax() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToUINT8WithinRangeMaxData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 254 }, res.get(0));
    }

    @Test
    public void testJsonNumberToUINT8OutOfMinRange() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Num_-1");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Num_-1"), colInfos);
        try {
            decoder.convertJson(myObj);
            fail();
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "unexpected exception message from UINT8 out of range",
                    "Insertion value out of range. ParStream type: UINT8. Value range [0,254]. Attempted value for insertion -1",
                    expected.getMessage());
        }
    }

    @Test
    public void testJsonNumberToUINT8OutOfMaxRange() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        try {
            decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                    "JsonNumberToUINT8OutOfMaxRangeData.json"));
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: UINT8"));
            assertTrue("didnt find out of range exception", expected.getMessage().contains("[0,254]"));
        }
    }

    @Test
    public void testJsonNumberToUINT8Null() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Null");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT8, 0, 0);

        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Null"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    /**
     * Conversion from String to uint8 is not supported. Test should fail.
     */
    @Test
    public void testStringToUINT8() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "String");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("num", Type.UINT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "String"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to uint8 conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Shortdate",
                    "Incompatible datatypes for column (num). Database type is UINT8, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }

    /** UINT16 **/

    @Test
    public void testJsonNumberToUINT16WithinRangeMin() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Zero");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Zero"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 0 }, res.get(0));
    }

    @Test
    public void testJsonNumberToUINT16WithinRange() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Num_120");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Num_120"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 120 }, res.get(0));
    }

    @Test
    public void testJsonNumberToUINT16WithinRangeMax() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToUINT16WithinRangeMaxData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 65534 }, res.get(0));
    }

    @Test
    public void testJsonNumberToUINT16OutOfMinRange() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Num_-1");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Num_-1"), colInfos);
        try {
            decoder.convertJson(myObj);
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: UINT16"));
            assertTrue("didnt find out of range exception", expected.getMessage().contains("[0,65534]"));
        }
    }

    @Test
    public void testJsonNumberToUINT16OutOfMaxRange() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        try {
            decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                    "JsonNumberToUINT16OutOfMaxRangeData.json"));
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: UINT16"));
            assertTrue("didnt find out of range exception", expected.getMessage().contains("[0,65534]"));
        }
    }

    @Test
    public void testJsonNumberToUINT16Null() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Null");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Null"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    /**
     * Conversion from String to uint16 is not supported. Test should fail.
     */
    @Test
    public void testStringToUINT16() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "String");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("num", Type.UINT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "String"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to uint16 conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Shortdate",
                    "Incompatible datatypes for column (num). Database type is UINT16, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }

    /** UINT32 **/

    @Test
    public void testJsonNumberToUINT32WithinRangeMin() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Zero");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Zero"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 0l }, res.get(0));
    }

    @Test
    public void testJsonNumberToUINT32WithinRange() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Num_120");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Num_120"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 120l }, res.get(0));
    }

    @Test
    public void testJsonNumberToUINT32WithinRangeMax() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToUINT32WithinRangeMaxData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 4294967294l }, res.get(0));
    }

    @Test
    public void testJsonNumberToUINT32OutOfMinRange() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Num_-1");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Num_-1"), colInfos);
        try {
            decoder.convertJson(myObj);
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: UINT32"));
            assertTrue("didnt find out of range exception", expected.getMessage().contains("[0,4294967294]"));
        }
    }

    @Test
    public void testJsonNumberToUINT32OutOfMaxRange() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        try {
            decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                    "JsonNumberToUINT32OutOfMaxRangeData.json"));
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: UINT32"));
            assertTrue("didnt find out of range exception", expected.getMessage().contains("[0,4294967294]"));
        }
    }

    @Test
    public void testJsonNumberToUINT32Null() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Null");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Null"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    /**
     * Conversion from String to uint32 is not supported. Test should fail.
     */
    @Test
    public void testStringToUINT32() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "String");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("num", Type.UINT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "String"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to uint32 conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Shortdate",
                    "Incompatible datatypes for column (num). Database type is UINT32, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }

    /** UINT64 **/

    @Test
    public void testJsonNumberToUINT64WithinRangeMin() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Zero");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Zero"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 0l }, res.get(0));
    }

    @Test
    public void testJsonNumberToUINT64WithinRange() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Num_120");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Num_120"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 120l }, res.get(0));
    }

    @Test
    public void testJsonNumberToUINT64WithinRangeMax() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToUINT64WithinRangeMaxData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 4294967294l }, res.get(0));
    }

    @Test
    public void testJsonNumberToUINT64OutOfMinRange() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Num_-1");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Num_-1"), colInfos);
        try {
            decoder.convertJson(myObj);
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: UINT64"));
            assertTrue("didnt find out of range exception", expected.getMessage().contains("[0,4294967294]"));
        }
    }

    @Test
    public void testJsonNumberToUINT64OutOfMaxRange() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        try {
            decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                    "JsonNumberToUINT64OutOfMaxRangeData.json"));
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: UINT64"));
            assertTrue("didnt find out of range exception", expected.getMessage().contains("[0,4294967294]"));
        }
    }

    @Test
    public void testJsonNumberToUINT64Null() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Null");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.UINT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Null"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    /**
     * Conversion from String to uint64 is not supported. Test should fail.
     */
    @Test
    public void testStringToUINT64() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "String");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("num", Type.UINT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "String"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to uint64 conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Shortdate",
                    "Incompatible datatypes for column (num). Database type is UINT64, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }

    /** INT8 **/

    @Test
    public void testJsonNumberToINT8WithinRangeMin() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToINT8WithinRangeMinData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { -128 }, res.get(0));
    }

    @Test
    public void testJsonNumberToINT8WithinRange() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Num_120");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Num_120"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 120 }, res.get(0));
    }

    @Test
    public void testJsonNumberToINT8WithinRangeMax() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToINT8WithinRangeMaxData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 126 }, res.get(0));
    }

    @Test
    public void testJsonNumberToINT8OutOfMinRange() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        try {
            decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                    "JsonNumberToINT8OutOfMinRangeData.json"));
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: INT8"));
            assertTrue("didnt find out of range exception", expected.getMessage().contains("[-128,126]"));
        }
    }

    @Test
    public void testJsonNumberToINT8OutOfMaxRange() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        try {
            decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                    "JsonNumberToINT8OutOfMaxRangeData.json"));
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: INT8"));
            assertTrue("didnt find out of range exception", expected.getMessage().contains("[-128,126]"));
        }
    }

    @Test
    public void testJsonNumberToINT8Null() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Null");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Null"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    /**
     * Conversion from String to int8 is not supported. Test should fail.
     */
    @Test
    public void testStringToINT8() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "String");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("num", Type.INT8, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "String"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to int8 conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Shortdate",
                    "Incompatible datatypes for column (num). Database type is INT8, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }

    /** INT16 **/

    @Test
    public void testJsonNumberToINT16WithinRangeMin() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToINT16WithinRangeMinData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { -32768 }, res.get(0));
    }

    @Test
    public void testJsonNumberToINT16WithinRange() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Num_120");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Num_120"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 120 }, res.get(0));
    }

    @Test
    public void testJsonNumberToINT16WithinRangeMax() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToINT16WithinRangeMaxData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 32766 }, res.get(0));
    }

    @Test
    public void testJsonNumberToINT16OutOfMinRange() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        try {
            decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                    "JsonNumberToINT16OutOfMinRangeData.json"));
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: INT16"));
            assertTrue("didnt find out of range exception", expected.getMessage().contains("[-32768,32766]"));
        }
    }

    @Test
    public void testJsonNumberToINT16OutOfMaxRange() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        try {
            decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                    "JsonNumberToINT16OutOfMaxRangeData.json"));
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: INT16"));
            assertTrue("didnt find out of range exception", expected.getMessage().contains("[-32768,32766]"));
        }
    }

    @Test
    public void testJsonNumberToINT16Null() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Null");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Null"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    /**
     * Conversion from String to int16 is not supported. Test should fail.
     */
    @Test
    public void testStringToINT16() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "String");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("num", Type.INT16, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "String"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to int16 conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Shortdate",
                    "Incompatible datatypes for column (num). Database type is INT16, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }

    /** INT32 **/

    @Test
    public void testJsonNumberToINT32WithinRangeMin() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToINT32WithinRangeMinData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { -2147483648 }, res.get(0));
    }

    @Test
    public void testJsonNumberToINT32WithinRange() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Num_120");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Num_120"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 120 }, res.get(0));
    }

    @Test
    public void testJsonNumberToINT32WithinRangeMax() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToINT32WithinRangeMaxData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 2147483646 }, res.get(0));
    }

    @Test
    public void testJsonNumberToINT32OutOfMinRange() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        try {
            decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                    "JsonNumberToINT32OutOfMinRangeData.json"));
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: INT32"));
            assertTrue("didnt find out of range exception", expected.getMessage().contains("[-2147483648,2147483646]"));
        }
    }

    @Test
    public void testJsonNumberToINT32OutOfMaxRange() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        try {
            decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                    "JsonNumberToINT32OutOfMaxRangeData.json"));
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: INT32"));
            assertTrue("didnt find out of range exception", expected.getMessage().contains("[-2147483648,2147483646]"));
        }
    }

    @Test
    public void testJsonNumberToINT32Null() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Null");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Null"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    /**
     * Conversion from String to int32 is not supported. Test should fail.
     */
    @Test
    public void testStringToINT32() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "String");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("num", Type.INT32, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "String"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to int32 conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Shortdate",
                    "Incompatible datatypes for column (num). Database type is INT32, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }

    /** INT64 **/

    @Test
    public void testJsonNumberToINT64WithinRangeMin() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToINT64WithinRangeMinData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { -9223372036854775808l }, res.get(0));
    }

    @Test
    public void testJsonNumberToINT64WithinRange() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Num_120");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Num_120"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 120l }, res.get(0));
    }

    @Test
    public void testJsonNumberToINT64WithinRangeMax() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToINT64WithinRangeMaxData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 9223372036854775806l }, res.get(0));
    }

    @Ignore
    public void testJsonNumberToINT64OutOfMinRange() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        try {
            decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                    "JsonNumberToINT64OutOfMinRangeData.json"));
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: INT64"));
            assertTrue("didnt find out of range exception",
                    expected.getMessage().contains("[-9223372036854775808,9223372036854775806]"));
        }
    }

    @Test
    public void testJsonNumberToINT64OutOfMaxRange() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        try {
            decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                    "JsonNumberToINT64OutOfMaxRangeData.json"));
            fail();
        } catch (JsonAdaptorException expected) {
            assertTrue("didnt find out of range exception",
                    expected.getMessage().startsWith("Insertion value out of range. ParStream type: INT64"));
            assertTrue("didnt find out of range exception",
                    expected.getMessage().contains("[-9223372036854775808,9223372036854775806]"));
        }
    }

    @Test
    public void testJsonNumberToINT64Null() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Null");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.INT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Null"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    /**
     * Conversion from String to int64 is not supported. Test should fail.
     */
    @Test
    public void testStringToINT64() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "String");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("num", Type.INT64, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "String"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to int64 conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Shortdate",
                    "Incompatible datatypes for column (num). Database type is INT64, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }

    /** DOUBLE **/
    @Test
    public void testJsonNumberToDouble() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.DOUBLE, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToDoubleData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 23.45d }, res.get(0));
    }

    @Test
    public void testJsonNumberToDoubleNull() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Null");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.DOUBLE, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Null"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    /**
     * Conversion from String to double is not supported. Test should fail.
     */
    @Test
    public void testStringToDouble() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "String");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("num", Type.DOUBLE, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "String"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to double conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Double",
                    "Incompatible datatypes for column (num). Database type is DOUBLE, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }

    /** FLOAT **/
    @Test
    public void testJsonNumberToFloat() throws Exception {
        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.FLOAT, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Various"), colInfos);
        List<Object[]> res = decoder.convertJson(AdaptorTestUtils.getJsonObj(className, "Various",
                "JsonNumberToFloatData.json"));

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { 23.45f }, res.get(0));
    }

    @Test
    public void testJsonNumberToFloatNull() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "Null");

        ColumnInfo[] colInfos = new ColumnInfo[1];
        colInfos[0] = AdaptorTestUtils.constructColumnInfo("intCol", Type.FLOAT, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "Null"), colInfos);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        assertArrayEquals("Object[] in result set", new Object[] { null }, res.get(0));
    }

    /**
     * Conversion from String to float is not supported. Test should fail.
     */
    @Test
    public void testStringToFloatFloat() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "String");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("num", Type.FLOAT, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "String"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to float conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Float",
                    "Incompatible datatypes for column (num). Database type is FLOAT, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }
}
