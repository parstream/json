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
package com.parstream.adaptor.json;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import javax.json.JsonArray;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonValue;
import javax.json.JsonValue.ValueType;

import com.parstream.driver.ColumnInfo;
import com.parstream.driver.ColumnInfo.Type;
import com.parstream.driver.ParstreamDate;
import com.parstream.driver.ParstreamException;
import com.parstream.driver.ParstreamShortDate;
import com.parstream.driver.ParstreamTime;
import com.parstream.driver.ParstreamTimestamp;

/**
 * This adaptor converts JSON objects to ParStream rows. A single JSON object is
 * decoded into a list of ParStream rows. The produced rows can be imported into
 * a ParStream database using the ParStream Java Streaming Import Interface.
 * <p>
 * The adaptor requires a configuration file, which defines exactly which JSON
 * keys are mapped to which columns in a ParStream table. This file is read as
 * java Properties files. <br>
 * The property keys defines the name of the table column and the property
 * values defines the absolute paths of the used JSON key name. The following is
 * a sample line in the configuration file.
 * 
 * <pre>
 * column.ID = keyNameId
 * column.psColumnName = objectName.keyNameValue
 * </pre>
 */
public class JsonAdaptor {

    private static final String ERROR_NULL_COLUMN_INFO = "ColumnInfo must not be null";
    private static final String ERROR_NULL_INPUT_STREAM = "input stream must not be null";
    private static final String ERROR_NULL_MAP_FILE = "config file path must not be null";

    private ColumnInfo[] _columnInfo;
    private Properties _mappingProps;
    private Map<String, Boolean> _usedColumnsInConfig;

    /**
     * Constructs a new JSON Decoder for ParStream. It also reads the
     * configuration file that specifies which JSON key is matched to which
     * ParStream column.
     * 
     * @param configFileStream
     *            input stream of configuration file, that specifies the
     *            JSON-key to ParStream-column mapping.
     * @param columnInfo
     *            the array defining the columns of the target ParStream table.
     *            The ParStream Java Streaming Import Interface must be used to
     *            obtain this array. Must not be null
     * @throws JsonAdaptorException
     *             in case the configuration file does not contain the
     *             sufficient information to perform the start the decoding
     *             process.
     * @throws IOException
     *             when an error occurs while reading from the provided mapping
     *             file stream
     */
    public JsonAdaptor(InputStream configFileStream, ColumnInfo[] columnInfo) throws JsonAdaptorException, IOException {
        assert configFileStream != null : ERROR_NULL_INPUT_STREAM;
        assert columnInfo != null : ERROR_NULL_COLUMN_INFO;

        initialize(configFileStream, columnInfo);
    }

    /**
     * Constructs a new JSON Decoder for ParStream. It also reads the
     * configuration file that specifies which JSON key is matched to which
     * ParStream column.
     * 
     * @param configFile
     *            the configuration file, that specifies the JSON-key to
     *            ParStream-column mapping. Must point to an existing and valid
     *            file
     * @param columnInfo
     *            the array defining the columns of the target ParStream table.
     *            The ParStream Java Streaming Import Interface must be used to
     *            obtain this array. Must not be null or empty.
     * @throws JsonAdaptorException
     *             in case the configuration file is a non-existent file or does
     *             not contain the sufficient information to perform the start
     *             the decoding process.
     * @throws IOException
     *             when an error occurs while reading from the provided mapping
     *             file
     */
    public JsonAdaptor(File configFile, ColumnInfo[] columnInfo) throws JsonAdaptorException, IOException {
        assert configFile != null : ERROR_NULL_MAP_FILE;
        assert columnInfo != null : ERROR_NULL_COLUMN_INFO;

        FileInputStream configInputStream = new FileInputStream(configFile);
        try {
            initialize(configInputStream, columnInfo);
        } catch (IOException e) {
            throw e;
        } finally {
            configInputStream.close();
        }
    }

    private void initialize(InputStream inputStream, ColumnInfo[] columnInfo) throws IOException, JsonAdaptorException {
        _mappingProps = new Properties();
        _mappingProps.load(inputStream);

        if (_mappingProps.size() == 0) {
            throw new JsonAdaptorException("no column mappings specified");
        }

        _columnInfo = columnInfo;
        _usedColumnsInConfig = new HashMap<String, Boolean>(_mappingProps.size());
    }

    /**
     * This method converts a single JSON object into one or more ParStream
     * rows.
     * 
     * @param jsonObj
     *            the JSON object that will be converted into a ParStream format
     * @return a list of Object[], each Object[] representing a single row in a
     *         ParStream table
     * @throws JsonAdaptorException
     *             in case of data type incompatibility between the source JSON
     *             value and the target ParStream column type
     */
    public List<Object[]> convertJson(JsonObject jsonObj) throws JsonAdaptorException {
        if (jsonObj == null) {
            return new ArrayList<Object[]>(0);
        }

        return createRowSet(parseObject(jsonObj, null));
    }

    private Map<String, Object> parseObject(JsonObject jsonObj, String prefix) throws JsonAdaptorException {
        Map<String, Object> parsedJson = new HashMap<String, Object>(jsonObj.size());
        for (Entry<String, JsonValue> entry : jsonObj.entrySet()) {
            String newKey;
            if (prefix == null) {
                newKey = entry.getKey();
            } else {
                newKey = prefix + "." + entry.getKey();
            }

            switch (entry.getValue().getValueType()) {
            case ARRAY:
                parsedJson.putAll(parseArray(jsonObj.getJsonArray(entry.getKey()), newKey));
                break;
            case FALSE:
                parsedJson.put(newKey, false);
                break;
            case NULL:
                parsedJson.put(newKey, null);
                break;
            case NUMBER:
                parsedJson.put(newKey, jsonObj.getJsonNumber(entry.getKey()));
                break;
            case OBJECT:
                parsedJson.putAll(parseObject(jsonObj.getJsonObject(entry.getKey()), newKey));
                break;
            case STRING:
                parsedJson.put(newKey, jsonObj.getJsonString(entry.getKey()).getString());
                break;
            case TRUE:
                parsedJson.put(newKey, true);
                break;
            }
        }
        return parsedJson;
    }

    private Map<String, Object> parseArray(JsonArray jsonArr, String prefix) throws JsonAdaptorException {
        Map<String, Object> parsedArr = new HashMap<String, Object>(jsonArr.size());
        if (jsonArr.size() == 0) {
            return parsedArr;
        }

        JsonArrayValues arrayValues = new JsonArrayValues(jsonArr.size());
        for (int i = 0; i < jsonArr.size(); ++i) {
            ValueType valueType = jsonArr.get(i).getValueType();

            switch (valueType) {
            case ARRAY: {
                JsonArray jsonArray = jsonArr.getJsonArray(i);
                arrayValues.addHashMap(parseArray(jsonArray, prefix));
                break;
            }
            case FALSE: {
                Map<String, Boolean> booleanHashMap = new HashMap<String, Boolean>(1);
                booleanHashMap.put(prefix, false);
                arrayValues.addHashMap(booleanHashMap);
                break;
            }
            case NULL: {
                Map<String, Object> nullHashMap = new HashMap<String, Object>(1);
                nullHashMap.put(prefix, null);
                arrayValues.addHashMap(nullHashMap);
                break;
            }
            case NUMBER: {
                Map<String, JsonNumber> numberHashMap = new HashMap<String, JsonNumber>(1);
                numberHashMap.put(prefix, jsonArr.getJsonNumber(i));
                arrayValues.addHashMap(numberHashMap);
                break;
            }
            case OBJECT:
                arrayValues.addHashMap(parseObject(jsonArr.getJsonObject(i), prefix));
                break;
            case STRING: {
                Map<String, String> stringHashMap = new HashMap<String, String>(1);
                stringHashMap.put(prefix, jsonArr.getJsonString(i).getString());
                arrayValues.addHashMap(stringHashMap);
                break;
            }
            case TRUE: {
                Map<String, Boolean> booleanHashMap = new HashMap<String, Boolean>(1);
                booleanHashMap.put(prefix, true);
                arrayValues.addHashMap(booleanHashMap);
                break;
            }
            }
        }

        parsedArr.put(prefix, arrayValues);
        return parsedArr;
    }

    private List<Object[]> createRowSet(final Map<String, Object> inParsedJson) throws JsonAdaptorException {
        // Remove any key-value pairs from the parsedJson, if the key can not be
        // found in the column mappings
        Map<String, Object> parsedJson = removeUnusedKeys(inParsedJson);

        List<Map<String, Object>> finalList = unfoldJsonArrays(parsedJson);
        List<Object[]> listToInsert = new ArrayList<Object[]>(finalList.size());

        for (Map<String, Object> recordData : finalList) {
            Object[] insertValues = new Object[_columnInfo.length];

            for (int i = 0; i < _columnInfo.length; ++i) {
                String psColumnKey = _columnInfo[i].getName();
                String jsonKey = _mappingProps.getProperty("column." + psColumnKey);

                if (jsonKey == null) {
                    insertValues[i] = null;
                } else {
                    Object jsonValueObj = recordData.get(jsonKey);
                    if (jsonValueObj == null) {
                        insertValues[i] = null;
                    } else {
                        // incompatible datatype check
                        switch (_columnInfo[i].getType()) {
                        case BITVECTOR8: {
                            if (jsonValueObj instanceof Boolean) {
                                processBoolean(insertValues, i, jsonValueObj);
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case UINT8: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                int intValue = jsonNum.intValue();
                                if (intValue >= 0 && intValue <= 254) {
                                    insertValues[i] = intValue;
                                } else {
                                    throwOutOfRangeException(Type.UINT8, intValue);
                                }
                            } else if (jsonValueObj instanceof Boolean) {
                                processBoolean(insertValues, i, jsonValueObj);
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case UINT16: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                int intValue = jsonNum.intValue();
                                if (intValue >= 0 && intValue <= 65534) {
                                    insertValues[i] = intValue;
                                } else {
                                    throwOutOfRangeException(Type.UINT16, intValue);
                                }
                            } else if (jsonValueObj instanceof Boolean) {
                                processBoolean(insertValues, i, jsonValueObj);
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case UINT32: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                Long longValue = jsonNum.longValue();
                                if (longValue >= 0 && longValue <= 4294967294L) {
                                    insertValues[i] = longValue;
                                } else {
                                    throwOutOfRangeException(Type.UINT32, longValue);
                                }
                            } else if (jsonValueObj instanceof Boolean) {
                                processBoolean(insertValues, i, jsonValueObj);
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case UINT64: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                Long longValue = jsonNum.longValue();
                                if (longValue >= 0 && longValue <= 4294967294L) {
                                    insertValues[i] = longValue;
                                } else {
                                    throwOutOfRangeException(Type.UINT64, longValue);
                                }
                            } else if (jsonValueObj instanceof Boolean) {
                                processBoolean(insertValues, i, jsonValueObj);
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case INT8: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                int intValue = jsonNum.intValue();
                                if (intValue >= -128 && intValue <= 126) {
                                    insertValues[i] = intValue;
                                } else {
                                    throwOutOfRangeException(Type.INT8, intValue);
                                }
                            } else if (jsonValueObj instanceof Boolean) {
                                processBoolean(insertValues, i, jsonValueObj);
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case INT16: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                int intValue = jsonNum.intValue();
                                if (intValue >= -32768 && intValue <= 32766) {
                                    insertValues[i] = intValue;
                                } else {
                                    throwOutOfRangeException(Type.INT16, intValue);
                                }
                            } else if (jsonValueObj instanceof Boolean) {
                                processBoolean(insertValues, i, jsonValueObj);
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case INT32: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                int intValue = jsonNum.intValue();
                                if (intValue <= 2147483646) {
                                    insertValues[i] = intValue;
                                } else {
                                    throwOutOfRangeException(Type.INT32, intValue);
                                }
                            } else if (jsonValueObj instanceof Boolean) {
                                processBoolean(insertValues, i, jsonValueObj);
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case INT64: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                Long value = jsonNum.longValue();
                                if (value >= -9223372036854775808L && value <= 9223372036854775806L) {
                                    insertValues[i] = value;
                                } else {
                                    throwOutOfRangeException(Type.INT64, value);
                                }
                            } else if (jsonValueObj instanceof Boolean) {
                                processBoolean(insertValues, i, jsonValueObj);
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case DOUBLE: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                double doubleVal = jsonNum.doubleValue();
                                insertValues[i] = doubleVal;
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case FLOAT: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                insertValues[i] = new Float(jsonNum.doubleValue());
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case SHORTDATE: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                long dateVal = jsonNum.longValue();
                                try {
                                    insertValues[i] = new ParstreamShortDate(new Date(dateVal));
                                } catch (ParstreamException e) {
                                    throw new JsonAdaptorException(e.getMessage());
                                }
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case DATE: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                long dateVal = jsonNum.longValue();
                                try {
                                    insertValues[i] = new ParstreamDate(new Date(dateVal));
                                } catch (ParstreamException e) {
                                    throw new JsonAdaptorException(e.getMessage());
                                }
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case TIME: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                long dateVal = jsonNum.longValue();
                                try {
                                    insertValues[i] = new ParstreamTime(new Date(dateVal));
                                } catch (ParstreamException e) {
                                    throw new JsonAdaptorException(e.getMessage());
                                }
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case TIMESTAMP: {
                            if (jsonValueObj instanceof JsonNumber) {
                                JsonNumber jsonNum = (JsonNumber) jsonValueObj;
                                long dateVal = jsonNum.longValue();
                                try {
                                    insertValues[i] = new ParstreamTimestamp(new Date(dateVal));
                                } catch (ParstreamException e) {
                                    throw new JsonAdaptorException(e.getMessage());
                                }
                            } else {
                                throwIncompatibleTypeException(psColumnKey, _columnInfo[i].getType(),
                                        jsonValueObj.getClass(), jsonValueObj.toString());
                            }
                            break;
                        }
                        case VARSTRING: {
                            if (jsonValueObj instanceof String) {
                                insertValues[i] = jsonValueObj;
                            } else {
                                insertValues[i] = jsonValueObj.toString();
                            }
                            break;
                        }
                        case BLOB: {
                            throw new JsonAdaptorException("ParStream BLOB column type not supported for decoding");
                        }
                        default: {
                            throw new JsonAdaptorException("Unknown ParStream column type: " + _columnInfo[i].getType());
                        }
                        }
                    }
                }
            }
            listToInsert.add(insertValues);
        }
        return listToInsert;
    }

    private void throwIncompatibleTypeException(String psColumnKey, ColumnInfo.Type psType, Class<?> jsonValueClass,
            String jsonValue) throws JsonAdaptorException {
        throw new JsonAdaptorException("Incompatible datatypes for column (" + psColumnKey + "). Database type is "
                + psType.toString() + ", JAVA type is " + jsonValueClass + ", Value attempted for insertion: "
                + jsonValue);
    }

    private void throwOutOfRangeException(Type type, long value) throws JsonAdaptorException {
        String exMessage = "Insertion value out of range. ParStream type: " + type.toString() + ". ";
        if (type == Type.UINT8) {
            exMessage += "Value range [0,254]. ";
        } else if (type == Type.UINT16) {
            exMessage += "Value range [0,65534]. ";
        } else if (type == Type.UINT32) {
            exMessage += "Value range [0,4294967294]. ";
        } else if (type == Type.UINT64) {
            exMessage += "Value range [0,4294967294]. ";
        } else if (type == Type.INT8) {
            exMessage += "Value range [-128,126]. ";
        } else if (type == Type.INT16) {
            exMessage += "Value range [-32768,32766]. ";
        } else if (type == Type.INT32) {
            exMessage += "Value range [-2147483648,2147483646]. ";
        } else if (type == Type.INT64) {
            exMessage += "Value range [-9223372036854775808,9223372036854775806]. ";
        }
        exMessage += "Attempted value for insertion " + value;
        throw new JsonAdaptorException(exMessage);
    }

    private List<Map<String, Object>> unfoldJsonArrays(Map<String, Object> parsedJson) {
        List<Map<String, Object>> initialList = new ArrayList<Map<String, Object>>(1);
        List<Map<String, Object>> finalList = new ArrayList<Map<String, Object>>(10);
        initialList.add(parsedJson);

        // as long as initialList is not empty, keep iterating over, to remove
        // arrays from within, and expand
        while (initialList.size() != 0) {
            Map<String, Object> currentHashMap = initialList.remove(0);
            // if the hashmap has no elements, no need to flatten or add it to
            // result set
            if (currentHashMap.size() > 0) {
                List<Map<String, Object>> flattened = expandOneArray(currentHashMap);
                if (flattened == null) {
                    finalList.add(currentHashMap);
                } else {
                    initialList.addAll(flattened);
                }
            }
        }
        return finalList;
    }

    /**
     * This method checks the given HashMap for any keys not used in the
     * ColumnMappins, and removes them accordingly from the HashMap.
     */
    private Map<String, Object> removeUnusedKeys(Map<String, Object> parsedJson) {
        for (Object jsonKey : parsedJson.keySet().toArray()) {
            Boolean exists = _usedColumnsInConfig.get(jsonKey);
            if (exists == null) {
                boolean found = false;
                for (Object jsonMappingKey : _mappingProps.values()) {
                    if (jsonMappingKey.toString().startsWith(jsonKey.toString())) {
                        found = true;
                        break;
                    }
                }
                if (found) {
                    _usedColumnsInConfig.put(jsonKey.toString(), true);
                } else {
                    _usedColumnsInConfig.put(jsonKey.toString(), false);
                    parsedJson.remove(jsonKey);
                }
            } else if (!exists) {
                parsedJson.remove(jsonKey);
            }
        }
        return parsedJson;
    }

    private List<Map<String, Object>> expandOneArray(Map<String, Object> hm) {
        String arrayListItemKey = null;
        for (Entry<String, Object> entry : hm.entrySet()) {
            if (entry.getValue() instanceof JsonArrayValues) {
                arrayListItemKey = entry.getKey();
            }
        }

        if (arrayListItemKey == null) {
            return null;
        }

        JsonArrayValues nestedItemJson = (JsonArrayValues) hm.get(arrayListItemKey);
        hm.remove(arrayListItemKey);

        List<Map<String, ?>> nestedItem = nestedItemJson.getArrayValues();

        List<Map<String, Object>> res = new ArrayList<Map<String, Object>>(nestedItem.size());
        if (nestedItem.size() == 0) {
            res.add(hm);
        } else {
            // initialize resulting arraylist, based on the arraylist size
            for (int i = 0; i < nestedItem.size(); ++i) {
                Map<String, Object> newHashMap = new HashMap<String, Object>(hm);

                // retrieve an object from the array list
                Object entryToAdd = nestedItem.get(i);
                if (entryToAdd instanceof HashMap) {
                    @SuppressWarnings("unchecked")
                    Map<String, Object> entryToAddHashMap = (Map<String, Object>) entryToAdd;
                    for (Entry<String, Object> entry : entryToAddHashMap.entrySet()) {
                        newHashMap.put(entry.getKey(), entry.getValue());
                    }
                }
                res.add(newHashMap);
            }
        }
        return res;
    }

    private void processBoolean(Object[] insertValues, int index, Object jsonValueObj) {
        if ((Boolean) jsonValueObj) {
            insertValues[index] = 1;
        } else {
            insertValues[index] = 0;
        }
    }
}
