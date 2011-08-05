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

import java.util.Map;

/**
 *
 */
class NullMetricsService implements MetricsService
  {
  @Override
  public void increment( String[] context, int amount )
    {
    }

  @Override
  public void set( String[] context, String value )
    {
    }

  @Override
  public void set( String[] context, int value )
    {
    }

  @Override
  public void set( String[] context, long value )
    {
    }

  @Override
  public String getString( String[] context )
    {
    return null;
    }

  @Override
  public int getInt( String[] context )
    {
    return 0;
    }

  @Override
  public long getLong( String[] context )
    {
    return 0;
    }

  @Override
  public boolean compareSet( String[] context, String isValue, String toValue )
    {
    return true;
    }

  @Override
  public boolean compareSet( String[] context, int isValue, int toValue )
    {
    return true;
    }

  @Override
  public boolean compareSet( String[] context, long isValue, long toValue )
    {
    return true;
    }

  @Override
  public void setProperties( Map<Object, Object> properties )
    {
    }

  @Override
  public void startService()
    {
    }

  @Override
  public void stopService()
    {
    }
  }
