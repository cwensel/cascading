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

package cascading.management;

import java.util.List;
import java.util.Map;

import cascading.provider.CascadingService;

/** Interface DocumentService provides coarse grained hooks for managing various statistics. */
public interface DocumentService extends CascadingService
  {
  String DOCUMENT_SERVICE_CLASS_PROPERTY = "cascading3.management.document.service.classname";

  void put( String key, Object object );

  void put( String type, String key, Object object );

  Map get( String type, String key );

  boolean supportsFind();

  List<Map<String, Object>> find( String type, String[] query );
  }