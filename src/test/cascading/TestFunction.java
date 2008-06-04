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

package cascading;

import cascading.operation.Function;
import cascading.operation.Operation;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleCollector;
import cascading.tuple.TupleEntry;

/** @version : IntelliJGuide,v 1.13 2001/03/22 22:35:22 SYSTEM Exp $ */
public class TestFunction extends Operation implements Function
  {
  private Tuple value;

  public TestFunction( Fields fieldDeclaration, Tuple value )
    {
    super( fieldDeclaration );
    this.value = value;
    }

  public void operate( TupleEntry input, TupleCollector outputCollector )
    {
    outputCollector.add( value );
    }
  }
