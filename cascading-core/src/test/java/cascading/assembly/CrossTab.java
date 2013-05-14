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

package cascading.assembly;

import cascading.operation.Aggregator;
import cascading.operation.BaseOperation;
import cascading.operation.Identity;
import cascading.operation.aggregator.First;
import cascading.operation.regex.RegexFilter;
import cascading.pipe.CoGroup;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tuple.Fields;

/**
 *
 */
public class CrossTab extends SubAssembly
  {
  /** Field serialVersionUID */
  private static final long serialVersionUID = 1L;

  /**
   * Constructor
   *
   * @param previous
   * @param argumentFieldSelector
   * @param crossTabOperation
   * @param fieldDeclaration
   */
  public CrossTab( Pipe previous, Fields argumentFieldSelector, CrossTabOperation crossTabOperation, Fields fieldDeclaration )
    {
    super( previous );

    // assert size of input
    Pipe pipe = new Each( previous, argumentFieldSelector, new Identity( new Fields( "n", "l", "v" ) ) );

    // name and rate against others of same movie
    pipe = new CoGroup( pipe, new Fields( "l" ), 1, new Fields( "n1", "l", "v1", "n2", "l2", "v2" ) );

    // remove useless fields
    pipe = new Each( pipe, new Fields( "l", "n1", "v1", "n2", "v2" ), new Identity() );

    // remove lines if the names are the same
    pipe = new Each( pipe, new RegexFilter( "^[^\\t]*\\t([^\\t]*)\\t[^\\t]*\\t\\1\\t.*", true ) );

    // transpose values in fields by natural sort order
    pipe = new Each( pipe, new SortElements( new Fields( "n1", "v1" ), new Fields( "n2", "v2" ) ) );

    // unique the pipe
    pipe = new GroupBy( pipe, Fields.ALL );

    pipe = new Every( pipe, Fields.ALL, new First(), Fields.RESULTS );

    // out: name1, name2, movie, name1, rate1, name2, rate2
    pipe = new GroupBy( pipe, new Fields( "n1", "n2" ) );

    // out: movie, name1, rate1, name2, rate2, score
    pipe = new Every( pipe, new Fields( "v1", "v2" ), crossTabOperation );

    pipe = new Each( pipe, new Identity( fieldDeclaration ) );

    setTails( pipe );
    }

  /** Class CrossTabOperation */
  public abstract static class CrossTabOperation<C> extends BaseOperation<C> implements Aggregator<C>
    {
    /**
     * Constructor CrossTabOperation creates a new CrossTabOperation instance.
     *
     * @param fields of type Fields
     */
    protected CrossTabOperation( Fields fields )
      {
      super( fields );
      }
    }
  }
