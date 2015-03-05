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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Date;
import java.util.List;

import javax.json.JsonObject;

import org.junit.Test;

import com.parstream.adaptor.json.JsonAdaptor;
import com.parstream.adaptor.json.JsonAdaptorException;
import com.parstream.adaptor.json.test.AdaptorTestUtils.Type;
import com.parstream.driver.ColumnInfo;
import com.parstream.driver.ParstreamDate;
import com.parstream.driver.ParstreamShortDate;
import com.parstream.driver.ParstreamTime;
import com.parstream.driver.ParstreamTimestamp;

public class DateTest {

    private static String className = "Date";

    /** SHORTDATE */

    @Test
    public void testLongToShortDate() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "LongToShortDate");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("shortdate", Type.SHORTDATE, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "LongToShortDate"), colInfo);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        Date expectedRes = new Date();
        expectedRes.setTime(1406889962520l); // 1406889962520 = Fri Aug 01
                                             // 12:46:02 CEST 2014

        assertTrue("ParstreamShortDate not identical", AdaptorTestUtils.isParstreamDateObjectIdentical(
                ParstreamShortDate.class, new ParstreamShortDate(expectedRes), (ParstreamShortDate) res.get(0)[0]));
    }

    /**
     * Conversion from String to shortdate is not supported. Test should fail.
     */
    @Test
    public void testStringToShortDate() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "StringToShortDate");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("shortdate", Type.SHORTDATE, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "StringToShortDate"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to shortdate conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Shortdate",
                    "Incompatible datatypes for column (shortdate). Database type is SHORTDATE, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }

    /** DATE */

    @Test
    public void testLongToDate() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "LongToDate");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("date", Type.DATE, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "LongToDate"), colInfo);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        Date expectedRes = new Date();
        expectedRes.setTime(1406889962520l); // 1406889962520 = Fri Aug 01
                                             // 12:46:02 CEST 2014

        assertTrue("ParstreamShortDate not identical", AdaptorTestUtils.isParstreamDateObjectIdentical(
                ParstreamDate.class, new ParstreamDate(expectedRes), (ParstreamDate) res.get(0)[0]));
    }

    /**
     * Conversion from String to date is not supported. Test should fail.
     */
    @Test
    public void testStringToDate() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "StringToDate");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("date", Type.DATE, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "StringToDate"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to date conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Shortdate",
                    "Incompatible datatypes for column (date). Database type is DATE, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }

    /** TIME */

    @Test
    public void testLongToTime() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "LongToTime");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("time", Type.TIME, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "LongToTime"), colInfo);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        Date expectedRes = new Date();
        expectedRes.setTime(1406889962520l); // 1406889962520 = Fri Aug 01
                                             // 12:46:02 CEST 2014

        assertTrue("ParstreamShortDate not identical", AdaptorTestUtils.isParstreamDateObjectIdentical(
                ParstreamTime.class, new ParstreamTime(expectedRes), (ParstreamTime) res.get(0)[0]));
    }

    /**
     * Conversion from String to time is not supported. Test should fail.
     */
    @Test
    public void testStringToTime() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "StringToTime");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("time", Type.TIME, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "StringToTime"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to time conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Shortdate",
                    "Incompatible datatypes for column (time). Database type is TIME, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }

    /** TIMESTAMP */

    @Test
    public void testLongToTimestamp() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "LongToTimestamp");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("timestamp", Type.TIMESTAMP, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "LongToTimestamp"), colInfo);
        List<Object[]> res = decoder.convertJson(myObj);

        assertEquals("result set size", 1, res.size());
        Date expectedRes = new Date();
        expectedRes.setTime(1406889962520l); // 1406889962520 = Fri Aug 01
                                             // 12:46:02 CEST 2014

        assertTrue("ParstreamShortDate not identical", AdaptorTestUtils.isParstreamDateObjectIdentical(
                ParstreamTimestamp.class, new ParstreamTimestamp(expectedRes), (ParstreamTimestamp) res.get(0)[0]));
    }

    /**
     * Conversion from String to timestamp is not supported. Test should fail.
     */
    @Test
    public void testStringToTimestamp() throws Exception {
        JsonObject myObj = AdaptorTestUtils.getJsonObj(className, "StringToTimestamp");

        ColumnInfo[] colInfo = new ColumnInfo[1];
        colInfo[0] = AdaptorTestUtils.constructColumnInfo("timestamp", Type.TIMESTAMP, 0, 0);
        JsonAdaptor decoder = new JsonAdaptor(AdaptorTestUtils.getIniFile(className, "StringToTimestamp"), colInfo);
        try {
            decoder.convertJson(myObj);
            fail("string to timestamp conversion didnt throw expected exception");
        } catch (JsonAdaptorException expected) {
            assertEquals(
                    "incorrect exception message for conversion from String to Shortdate",
                    "Incompatible datatypes for column (timestamp). Database type is TIMESTAMP, JAVA type is class java.lang.String, Value attempted for insertion: sjkfghsd",
                    expected.getMessage());
        }
    }
}
