/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.property;

import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/** Class PropertyUtil is a static helper class for handling properties. */
public class PropertyUtil
  {
  public static String getProperty( Map<Object, Object> properties, String key )
    {
    return getProperty( properties, key, (String) null );
    }

  public static String getStringProperty( Properties defaultProperties, Map<Object, Object> properties, String property )
    {
    return getProperty( properties, property, defaultProperties.getProperty( property ) );
    }

  public static <A> A getProperty( Map<Object, Object> properties, String key, A defaultValue )
    {
    if( properties == null )
      return defaultValue;

    A value;

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

  public static Properties createProperties( Iterable<Map.Entry<String, String>> defaultProperties )
    {
    Properties properties = new Properties();

    for( Map.Entry<String, String> property : defaultProperties )
      properties.setProperty( property.getKey(), property.getValue() );

    return properties;
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