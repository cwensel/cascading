/*
 * Copyright (c) 2007-2008 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.assembly;

import cascading.operation.Aggregator;
import cascading.operation.BaseOperation;
import cascading.operation.Cut;
import cascading.operation.Identity;
import cascading.operation.aggregator.First;
import cascading.operation.regex.RegexFilter;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.Group;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.PipeAssembly;
import cascading.tuple.Fields;

/**
 *
 */
public class CrossTab extends PipeAssembly
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
    // assert size of input
    Pipe pipe = new Each( previous, argumentFieldSelector, new Identity( new Fields( "n", "l", "v" ) ) );

    // name and rate against others of same movie
    pipe = new Group( pipe, new Fields( "l" ), 2, new Fields( "n1", "l", "v1", "n2", "l2", "v2" ) );

    // remove useless fields
    pipe = new Each( pipe, new Cut( new Fields( "l", "n1", "v1", "n2", "v2" ) ) );

    // remove lines if the names are the same
    pipe = new Each( pipe, new RegexFilter( "^[^\\t]*\\t([^\\t]*)\\t[^\\t]*\\t\\1\\t.*", true ) );

    // transpose values in fields by natural sort order
    pipe = new Each( pipe, new SortElements( new Fields( "n1", "v1" ), new Fields( "n2", "v2" ) ) );

    // unique the pipe
    pipe = new GroupBy( pipe, Fields.ALL );

    pipe = new Every( pipe, Fields.ALL, new First(), Fields.RESULTS );

    // out: name1, name2, movie, name1, rate1, name2, rate2
    pipe = new Group( pipe, new Fields( "n1", "n2" ) );

    // out: movie, name1, rate1, name2, rate2, score
    pipe = new Every( pipe, new Fields( "v1", "v2" ), crossTabOperation );

    pipe = new Each( pipe, new Identity( fieldDeclaration ) );

    setTails( pipe );
    }

  /** Class CrossTabOperation */
  public abstract static class CrossTabOperation extends BaseOperation implements Aggregator
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
