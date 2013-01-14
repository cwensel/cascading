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

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * The ConfigDef class allows for the creation of a configuration properties template to be applied to an existing
 * properties configuration set.
 * <p/>
 * There are three property modes, {@link Mode#DEFAULT}, {@link Mode#REPLACE}, and {@link Mode#UPDATE}.
 * <p/>
 * <ul>
 * <li>A DEFAULT property is only applied if there is no existing value in the property set.</li>
 * <li>A REPLACE property is always applied overriding any previous values.</li>
 * <li>An UPDATE property is always applied to an existing property. Usually when the property key represent a list of values.</li>
 * </ul>
 */
public class ConfigDef implements Serializable
  {
  public enum Mode
    {
      DEFAULT, REPLACE, UPDATE
    }

  public interface Setter
    {
    String set( String key, String value );

    String update( String key, String value );

    String get( String key );
    }

  public interface Getter
    {
    String update( String key, String value );

    String get( String key );
    }

  protected Map<Mode, Map<String, String>> config;

  public ConfigDef()
    {
    }

  /**
   * Method setProperty sets the value to the given key using the {@link Mode#REPLACE} mode.
   *
   * @param key   the key
   * @param value the value
   * @return the current ConfigDef instance
   */
  public ConfigDef setProperty( String key, String value )
    {
    return setProperty( Mode.REPLACE, key, value );
    }

  /**
   * Method setProperty sets the value to the given key using the given {@link Mode} value.
   *
   * @param key   the key
   * @param value the value
   * @return the current ConfigDef instance
   */
  public ConfigDef setProperty( Mode mode, String key, String value )
    {
    getMode( mode ).put( key, value );

    return this;
    }

  protected Map<String, String> getMode( Mode mode )
    {
    if( config == null )
      config = new HashMap<Mode, Map<String, String>>();

    if( !config.containsKey( mode ) )
      config.put( mode, new HashMap<String, String>() );

    return config.get( mode );
    }

  protected Map<String, String> getModeSafe( Mode mode )
    {
    if( config == null )
      return Collections.EMPTY_MAP;

    if( !config.containsKey( mode ) )
      return Collections.EMPTY_MAP;

    return config.get( mode );
    }

  /**
   * Returns {@code true} if there are no properties.
   *
   * @return true if no properties.
   */
  public boolean isEmpty()
    {
    return config == null || config.isEmpty();
    }

  public String apply( String key, Getter getter )
    {
    String defaultValue = getModeSafe( Mode.DEFAULT ).get( key );
    String replaceValue = getModeSafe( Mode.REPLACE ).get( key );
    String updateValue = getModeSafe( Mode.UPDATE ).get( key );

    String currentValue = getter.get( key );

    if( currentValue == null && replaceValue == null && updateValue == null )
      return defaultValue;

    if( replaceValue != null )
      return replaceValue;

    if( updateValue == null )
      return currentValue;

    if( currentValue == null )
      return updateValue;

    return getter.update( key, updateValue );
    }

  public void apply( Mode mode, Setter setter )
    {
    if( !config.containsKey( mode ) )
      return;

    for( String key : config.get( mode ).keySet() )
      {
      switch( mode )
        {
        case DEFAULT:
          if( setter.get( key ) == null )
            setter.set( key, config.get( mode ).get( key ) );
          break;
        case REPLACE:
          setter.set( key, config.get( mode ).get( key ) );
          break;
        case UPDATE:
          setter.update( key, config.get( mode ).get( key ) );
          break;
        }
      }
    }

  public Collection<String> getAllKeys()
    {
    Set<String> keys = new HashSet<String>();

    for( Map<String, String> map : config.values() )
      keys.addAll( map.keySet() );

    return Collections.unmodifiableSet( keys );
    }
  }
