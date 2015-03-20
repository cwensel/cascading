/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import cascading.provider.CascadingService;

/** Interface MetricsService provides fine grained hooks for managing various metrics. */
public interface MetricsService extends CascadingService
  {
  String METRICS_SERVICE_CLASS_PROPERTY = "cascading.management.metrics.service.classname";

  void increment( String[] context, int amount );

  void set( String[] context, String value );

  void set( String[] context, int value );

  void set( String[] context, long value );

  String getString( String[] context );

  int getInt( String[] context );

  long getLong( String[] context );

  boolean compareSet( String[] context, String isValue, String toValue );

  boolean compareSet( String[] context, int isValue, int toValue );

  boolean compareSet( String[] context, long isValue, long toValue );
  }