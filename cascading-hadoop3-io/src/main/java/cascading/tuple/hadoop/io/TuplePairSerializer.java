/*
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

package cascading.tuple.hadoop.io;

import java.io.IOException;

import cascading.tuple.hadoop.TupleSerialization;
import cascading.tuple.io.TupleOutputStream;
import cascading.tuple.io.TuplePair;

public class TuplePairSerializer extends BaseSerializer<TuplePair>
  {
  private final TupleOutputStream.TupleElementWriter[] keyWriters;
  private final TupleOutputStream.TupleElementWriter[] sortWriters;

  public TuplePairSerializer( TupleSerialization.SerializationElementWriter elementWriter )
    {
    super( elementWriter );

    Class[] keyClasses = elementWriter.getTupleSerialization().getKeyTypes();
    Class[] sortClasses = elementWriter.getTupleSerialization().getSortTypes();

    if( elementWriter.getTupleSerialization().areTypesRequired() )
      {
      if( keyClasses == null )
        throw new IllegalStateException( "types are required to perform serialization, grouping declared fields: " + elementWriter.getTupleSerialization().getKeyFields() );

      if( sortClasses == null )
        throw new IllegalStateException( "types are required to perform serialization, sorting declared fields: " + elementWriter.getTupleSerialization().getSortFields() );
      }

    keyWriters = HadoopTupleOutputStream.getWritersFor( elementWriter, keyClasses );
    sortWriters = HadoopTupleOutputStream.getWritersFor( elementWriter, sortClasses );
    }

  public void serialize( TuplePair tuple ) throws IOException
    {
    if( keyWriters == null )
      outputStream.writeUnTyped( tuple.getLhs() );
    else
      outputStream.writeWith( keyWriters, tuple.getLhs() );

    if( sortWriters == null )
      outputStream.writeUnTyped( tuple.getRhs() );
    else
      outputStream.writeWith( sortWriters, tuple.getRhs() );
    }
  }
