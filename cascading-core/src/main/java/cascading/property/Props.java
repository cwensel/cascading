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

import java.util.Map;
import java.util.Properties;

/**
 * Class Props is the base class for frameworks specific properties helper classes.
 * <p>
 * Use the sub-classes to either create a {@link Properties} instance with custom or default values to be passed
 * to any sub-system that requires a Map or Properties instance of properties and values.
 * <p>
 * Note some Props sub-classes have static accessors. It is recommended the fluent instance methods be used instead
 * of the static methods. All static accessors may be deprecated in future versions.
 */
public abstract class Props
  {
  /**
   * Method buildProperties returns a new {@link Properties} instance with all property values for this type.
   * <p>
   * If no values have been set, all default properties and values will be returned.
   *
   * @return a new Properties instance
   */
  public Properties buildProperties()
    {
    return buildProperties( (Properties) null );
    }

  /**
   * Method buildProperties returns a new {@link Properties} instance with all property values for this type
   * using the given Map of property values as defaults. The given Map will not be modified.
   * <p>
   * If no values have been set, all default properties and values will be returned.
   *
   * @return a new Properties instance
   */
  public Properties buildProperties( Map<Object, Object> defaultProperties )
    {
    return buildProperties( PropertyUtil.createProperties( defaultProperties, null ) );
    }

  /**
   * Method buildProperties returns a new {@link Properties} instance with all property values for this type
   * using the given Iterable<Map.Entry<String, String>> of property values as defaults. The given Iterable will not be modified.
   * <p>
   * If no values have been set, all default properties and values will be returned.
   *
   * @return a new Properties instance
   */
  public Properties buildProperties( Iterable<Map.Entry<String, String>> defaultProperties )
    {
    return buildProperties( PropertyUtil.createProperties( defaultProperties ) );
    }

  /**
   * Method buildProperties returns a new {@link Properties} instance with all property values for this type
   * using the given Properties instance of property values as defaults. The given Map will not be modified.
   * <p>
   * If no values have been set, all default properties and values will be returned.
   *
   * @return a new Properties instance
   */
  public Properties buildProperties( Properties defaultProperties )
    {
    defaultProperties = defaultProperties != null ? new Properties( defaultProperties ) : new Properties();

    addPropertiesTo( defaultProperties );

    return defaultProperties;
    }

  public ConfigDef setProperties( ConfigDef configDef )
    {
    return setProperties( configDef, ConfigDef.Mode.REPLACE );
    }

  public ConfigDef setProperties( ConfigDef configDef, ConfigDef.Mode mode )
    {
    Properties properties = buildProperties();

    for( String name : properties.stringPropertyNames() )
      configDef.setProperty( mode, name, properties.getProperty( name ) );

    return configDef;
    }

  protected abstract void addPropertiesTo( Properties properties );
  }
