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

package cascading.util;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class PropertyUtil
  {
  public static String getProperty( Map<Object, Object> properties, String key )
    {
    return getProperty( properties, key, (String) null );
    }

  public static <A> A getProperty( Map<Object, Object> properties, String key, A defaultValue )
    {
    if( properties == null )
      return defaultValue;

    A value = null;

    if( properties instanceof Properties )
      value = (A) ( (Properties) properties ).getProperty( key );
    else
      value = (A) properties.get( key );

    return value == null ? defaultValue : value;
    }

  public static void setProperty( Map<Object, Object> properties, String key, String value )
    {
    if( properties == null || value == null || value.isEmpty() )
      return;

    if( properties instanceof Properties )
      ( (Properties) properties ).setProperty( key, value );
    else
      properties.put( key, value );
    }

  public static Properties createProperties( Map<Object, Object> properties, Properties defaultProperties )
    {
    Properties results = defaultProperties == null ? new Properties() : new Properties( defaultProperties );

    if( properties == null )
      return results;

    Set<Object> keys = new HashSet<Object>( properties.keySet() );

    // keys will only be grabbed if both key/value are String, so keep orig keys
    if( properties instanceof Properties )
      keys.addAll( ( (Properties) properties ).stringPropertyNames() );

    for( Object key : keys )
      {
      Object value = properties.get( key );

      if( value == null && properties instanceof Properties && key instanceof String )
        value = ( (Properties) properties ).getProperty( (String) key );

      if( value == null ) // don't stuff null values
        continue;

      // don't let these objects pass, even though toString is called below.
      if( value instanceof Class )
        continue;

      results.setProperty( key.toString(), value.toString() );
      }

    return results;
    }
  }