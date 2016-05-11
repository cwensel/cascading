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

package cascading.flow.stream.element;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.flow.FlowProcessWrapper;
import cascading.property.ConfigDef;

/**
 *
 */
public class ElementFlowProcess extends FlowProcessWrapper
  {
  private final ConfigDef configDef;
  private final ConfigDef.Getter getter;

  public ElementFlowProcess( FlowProcess flowProcess, ConfigDef configDef )
    {
    super( flowProcess );

    this.configDef = configDef;
    this.getter = new ConfigDef.Getter()
    {
    @Override
    public String update( String key, String value )
      {
      String result = get( key );

      if( result == null )
        return value;

      if( result.contains( value ) )
        return result;

      return result + "," + value;
      }

    @Override
    public String get( String key )
      {
      Object value = getDelegate().getProperty( key );

      if( value == null )
        return null;

      return value.toString();
      }
    };
    }

  @Override
  public Object getProperty( String key )
    {
    return configDef.apply( key, getter );
    }

  @Override
  public Collection<String> getPropertyKeys()
    {
    Set<String> keys = new HashSet<String>();

    keys.addAll( getDelegate().getPropertyKeys() );
    keys.addAll( configDef.getAllKeys() );

    return Collections.unmodifiableSet( keys );
    }
  }
