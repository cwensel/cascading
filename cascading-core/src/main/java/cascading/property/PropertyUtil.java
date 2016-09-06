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

package cascading.property;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import cascading.flow.planner.ScopedElement;

import static cascading.util.Util.isEmpty;

/** Class PropertyUtil is a static helper class for handling properties. */
public class PropertyUtil
  {
  public static String getProperty( Map<Object, Object> properties, String property )
    {
    return getProperty( properties, property, (String) null );
    }

  public static boolean getBooleanProperty( Map<Object, Object> properties, String property, boolean defaultValue )
    {
    return getBooleanProperty( System.getProperties(), properties, property, defaultValue );
    }

  public static boolean getBooleanProperty( Properties defaultProperties, Map<Object, Object> properties, String property, boolean defaultValue )
    {
    String result;

    if( defaultProperties == null )
      result = getProperty( properties, property );
    else
      result = getProperty( properties, property, defaultProperties.getProperty( property ) );

    if( isEmpty( result ) )
      return defaultValue;

    return Boolean.parseBoolean( result );
    }

  public static int getIntProperty( Map<Object, Object> properties, String property, int defaultValue )
    {
    return getIntProperty( System.getProperties(), properties, property, defaultValue );
    }

  public static int getIntProperty( Properties defaultProperties, Map<Object, Object> properties, String property, int defaultValue )
    {
    String result;

    if( defaultProperties == null )
      result = getProperty( properties, property );
    else
      result = getProperty( properties, property, defaultProperties.getProperty( property ) );

    if( isEmpty( result ) )
      return defaultValue;

    return Integer.parseInt( result );
    }

  public static String getStringProperty( Map<Object, Object> properties, String property )
    {
    return getStringProperty( System.getProperties(), properties, property );
    }

  public static String getStringProperty( Properties defaultProperties, Map<Object, Object> properties, String property )
    {
    if( defaultProperties == null )
      return getProperty( properties, property );

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

  public static String getProperty( final Map<Object, Object> properties, ScopedElement flowElement, String property )
    {
    if( flowElement != null && flowElement.hasConfigDef() )
      return PropertyUtil.getProperty( properties, flowElement.getConfigDef(), property );

    return PropertyUtil.getProperty( properties, property );
    }

  public static String getProperty( final Map<Object, Object> properties, ConfigDef configDef, String property )
    {
    return configDef.apply( property, new ConfigDef.Getter()
    {
    @Override
    public String update( String key, String value )
      {
      return value;
      }

    @Override
    public String get( String key )
      {
      return getProperty( properties, key );
      }
    } );
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

  public static Map<Object, Object> asFlatMap( Map<Object, Object> properties )
    {
    if( !( properties instanceof Properties ) )
      return properties;

    Map<Object, Object> map = new HashMap<>();
    Properties props = (Properties) properties;

    for( String property : props.stringPropertyNames() )
      map.put( property, props.getProperty( property ) );

    return map;
    }
  }