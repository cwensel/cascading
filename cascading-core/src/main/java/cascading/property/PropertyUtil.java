/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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
import java.util.Objects;
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

  /**
   * Merge all properties instances into one.
   * <p>
   * Always returns a new Properties instance.
   * <p>
   * Subsequent values to not overwrite earlier values, that is, the left most instances
   * have more precedence, that is, won't be replaced.
   *
   * @param properties
   * @return a new Properties instance.
   */
  public static Properties merge( Properties... properties )
    {
    Properties results = new Properties();

    for( int i = properties.length - 1; i >= 0; i-- )
      {
      Properties current = properties[ i ];

      if( current == null )
        continue;

      Set<String> currentNames = current.stringPropertyNames();

      for( String currentName : currentNames )
        results.setProperty( currentName, current.getProperty( currentName ) );
      }

    return results;
    }

  public static Properties remove( Properties properties, Properties... remove )
    {
    Objects.requireNonNull( properties, "properties may not be null" );

    Set<String> keys = new HashSet<>();

    for( Properties current : remove )
      keys.addAll( current.stringPropertyNames() );

    return remove( properties, keys );
    }

  public static Properties remove( Properties properties, Set<String> keys )
    {
    Objects.requireNonNull( properties, "properties may not be null" );

    Properties results = new Properties();

    for( String name : properties.stringPropertyNames() )
      {
      if( keys.contains( name ) )
        continue;

      results.setProperty( name, properties.getProperty( name ) );
      }

    return results;
    }

  public static Properties retain( Properties properties, Properties... retain )
    {
    Objects.requireNonNull( properties, "properties may not be null" );

    Set<String> keys = new HashSet<>();

    for( Properties current : retain )
      {
      if( current != null )
        keys.addAll( current.stringPropertyNames() );
      }

    return remove( properties, keys );
    }

  public static Properties retain( Properties properties, Set<String> keys )
    {
    Objects.requireNonNull( properties, "properties may not be null" );

    Properties results = new Properties();

    for( String name : properties.stringPropertyNames() )
      {
      if( !keys.contains( name ) )
        continue;

      results.setProperty( name, properties.getProperty( name ) );
      }

    return results;
    }
  }