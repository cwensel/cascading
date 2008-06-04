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

package cascading.examples;

import java.util.Set;
import java.util.TreeSet;

import cascading.operation.Function;
import cascading.operation.Operation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/** Class SortElements ... */
public class SortElements extends Operation implements Function
  {
  private final Fields[] fields;

  public SortElements( Fields... fields )
    {
    super( Fields.ARGS );
    this.fields = fields;
    }

  public void operate( TupleEntry input, TupleCollector outputCollector )
    {
    Set<Tuple> set = new TreeSet<Tuple>();

    for( Fields field : fields )
      set.add( input.selectTuple( field ) );

    int i = 0;
    for( Tuple tuple : set )
      input.getTuple().put( input.getFields(), fields[ i++ ], tuple );

    outputCollector.add( input );
    }
  }
