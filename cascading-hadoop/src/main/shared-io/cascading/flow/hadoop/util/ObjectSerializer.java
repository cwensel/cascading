/*
 * Copyright (c) 2007-2016 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
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

package cascading.flow.hadoop.util;

import java.io.IOException;

/**
 * Class ObjectSerializer is an experimental interface for allowing custom java.lang.Object subclass serialization
 * other than via the java.io.Serializable interface.
 * <p/>
 * To use, set the {@link #OBJECT_SERIALIZER_PROPERTY} value on the flow configuration.
 */
public interface ObjectSerializer
  {
  String OBJECT_SERIALIZER_PROPERTY = "cascading.util.serializer";

  <T> byte[] serialize( T object, boolean compress ) throws IOException;

  <T> T deserialize( byte[] bytes, Class<T> type, boolean decompress ) throws IOException;

  <T> boolean accepts( Class<T> type );
  }