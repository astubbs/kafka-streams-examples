/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.streams.zephyrstores;

import io.confluent.rest.exceptions.RestException;
import io.confluent.rest.exceptions.RestNotFoundException;

public class Errors {

  public static final String KEY_NOT_FOUND_MESSAGE = "Key '%s' not found in store '%s'.";
  public static final int KEY_NOT_FOUND_ERROR_CODE = 40401;

  public static RestException keyNotFoundException(String key, String storeName) {
    String message = String.format(KEY_NOT_FOUND_MESSAGE, key, storeName);
    return new RestNotFoundException(message, KEY_NOT_FOUND_ERROR_CODE);
  }

  public static final String STORE_NOT_FOUND_MESSAGE = "Store '%s' not found.";
  public static final int STORE_NOT_FOUND_ERROR_CODE = 40402;

  public static RestException storeNotFoundException(String storeName) {
    String message = String.format(STORE_NOT_FOUND_MESSAGE, storeName);
    return new RestNotFoundException(message, STORE_NOT_FOUND_ERROR_CODE);
  }
}
