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

package cascading.operation.aggregator;

import java.beans.ConstructorProperties;

import cascading.operation.Aggregator;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class First is an {@link Aggregator} that returns the first {@link Tuple} encountered.
 * <p/>
 * By default, it returns the first Tuple of {@link Fields#ARGS} found.
 */
public class First extends ExtentBase
  {
  /** Selects and returns the first argument Tuple encountered. */
  public First()
    {
    super( Fields.ARGS );
    }

  /**
   * Selects and returns the first argument Tuple encountered.
   *
   * @param fieldDeclaration of type Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public First( Fields fieldDeclaration )
    {
    super( fieldDeclaration.size(), fieldDeclaration );
    }

  /**
   * Selects and returns the first argument Tuple encountered, unless the Tuple
   * is a member of the set ignoreTuples.
   *
   * @param fieldDeclaration of type Fields
   * @param ignoreTuples     of type Tuple...
   */
  @ConstructorProperties({"fieldDeclaration", "ignoreTuples"})
  public First( Fields fieldDeclaration, Tuple... ignoreTuples )
    {
    super( fieldDeclaration, ignoreTuples );
    }

  protected void performOperation( Tuple[] context, TupleEntry entry )
    {
    if( context[ 0 ] == null )
      context[ 0 ] = new Tuple( entry.getTuple() );
    }

  }
