/*
 * Copyright (c) 2007-2023 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.tap.parquet;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.tuple.Fields;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;

public class TypedTupleReadSupport extends TupleReadSupport
  {
  public TypedTupleReadSupport()
    {
    }

  @Override
  public ReadContext init( Configuration configuration, Map<String, String> keyValueMetaData, MessageType fileSchema )
    {
    Fields requestedFields = getRequestedFields( configuration );
    if( requestedFields == null || requestedFields.isAll() )
      {
      return new ReadContext( fileSchema );
      }
    else
      {
      SchemaIntersection intersection = new SchemaIntersection( fileSchema, requestedFields );

      Fields sourceFields = intersection.getSourceFields();

      // if any of the requested fields are not found, fail
      if( !sourceFields.equalsFields( requestedFields ) )
        {
        Set<String> sourceSet = safeNamesSet( sourceFields );
        Set<String> requestedSet = safeNamesSet( requestedFields );

        Sets.SetView<String> foundFields = Sets.intersection( sourceSet, requestedSet );
        Sets.SetView<String> missingFields = Sets.difference( requestedSet, sourceSet );

        if( !missingFields.isEmpty() )
          throw new IllegalArgumentException( "requested parquet fields were not found in the schema, not found: " + missingFields + ", found: " + foundFields );
        }

      return new ReadContext( intersection.getRequestedSchema() );
      }
    }

  protected static Set<String> safeNamesSet( Fields fields )
    {
    return new LinkedHashSet<>( safeNames( fields ) );
    }

  protected static List<String> safeNames( Fields fields )
    {
    List<String> names = new ArrayList<>( fields.size() );

    for( int i = 0; i <  fields.size(); i++ )
      {
      Comparable<?> comparable = fields.get( i );

      if( comparable == null )
          throw new IllegalArgumentException( "field has null name: " + fields.print() );

      if( comparable instanceof String )
          names.add( (String) comparable );
      else
          names.add( "$" + comparable );
      }

    return names;
    }
  }
