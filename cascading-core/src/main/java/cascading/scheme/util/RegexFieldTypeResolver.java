/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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

package cascading.scheme.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 *
 */
public class RegexFieldTypeResolver implements FieldTypeResolver
  {
  private final Map<String, Class> typeMap;
  private final Class defaultType;

  /** @param typeMap  */
  public RegexFieldTypeResolver( Map<String, Class> typeMap, Class defaultType )
    {
    this.typeMap = new LinkedHashMap<String, Class>( typeMap ); // preserve key order
    this.defaultType = defaultType;
    }

  @Override
  public Class inferTypeFrom( int ordinal, String fieldName )
    {
    for( Map.Entry<String, Class> entry : typeMap.entrySet() )
      {
      String pattern = entry.getKey();

      if( matches( pattern, fieldName ) )
        return entry.getValue();
      }

    return defaultType;
    }

  protected boolean matches( String pattern, String fieldName )
    {
    return fieldName.matches( pattern );
    }

  @Override
  public String prepareField( int i, String fieldName, Class type )
    {
    return null;
    }

  @Override
  public String cleanField( int ordinal, String fieldName, Class type )
    {
    return null;
    }
  }
