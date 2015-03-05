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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * An instance of this class encapsulates the values stored in a particular JSON
 * array.
 *
 */
public class JsonArrayValues {

    private List<Map<String, ?>> _arrayValues;

    /**
     * Constructs a new JsonArrayValues instance with the given size. The size
     * is used for the initial size of array items. It gets automatically
     * expanded should more array values get added.
     * 
     * @param size
     *            The array size
     */
    JsonArrayValues(int size) {
        _arrayValues = new ArrayList<Map<String, ?>>(size);
    }

    void addHashMap(Map<String, ?> hashMap) {
        _arrayValues.add(hashMap);
    }

    List<Map<String, ?>> getArrayValues() {
        return _arrayValues;
    }
}
