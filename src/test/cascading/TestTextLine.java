/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading;

import cascading.scheme.TextLine;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntryCollector;

/**
 *
 */
public class TestTextLine extends TextLine
  {

  public TestTextLine( Fields sourceFields )
    {
    super( sourceFields );
    }

  @Override
  public void source( Tuple tuple, TupleEntryCollector tupleEntryCollector )
    {
    tuple = new Tuple( (Object[]) tuple.get( 1 ).toString().split( "\t" ) );
    tupleEntryCollector.add( tuple );
    }
  }
