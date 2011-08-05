/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.management;

import cascading.util.CascadingService;

/**
 *
 */
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