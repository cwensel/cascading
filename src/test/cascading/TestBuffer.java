/*
 * Copyright (c) 2007-20010 Concurrent, Inc. All Rights Reserved.
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

import java.util.Iterator;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 *
 */
public class TestBuffer extends BaseOperation implements Buffer
  {
  private int exepectedSize = -1;
  private boolean insertHeader;
  private boolean insertFooter;
  private Comparable value;

  public TestBuffer( Fields fieldDeclaration, int exepectedSize, boolean insertHeader, boolean insertFooter, String value )
    {
    super( fieldDeclaration );
    this.exepectedSize = exepectedSize;
    this.insertHeader = insertHeader;
    this.insertFooter = insertFooter;
    this.value = value;
    }

  public TestBuffer( Fields fieldDeclaration, int exepectedSize, boolean insertHeader, String value )
    {
    super( fieldDeclaration );
    this.exepectedSize = exepectedSize;
    this.insertHeader = insertHeader;
    this.value = value;
    }

  public TestBuffer( Fields fieldDeclaration, boolean insertHeader, String value )
    {
    super( fieldDeclaration );
    this.insertHeader = insertHeader;
    this.value = value;
    }

  public TestBuffer( Fields fieldDeclaration, Comparable value )
    {
    super( fieldDeclaration );
    this.value = value;
    }

  public void operate( FlowProcess flowProcess, BufferCall bufferCall )
    {
    if( insertHeader )
      bufferCall.getOutputCollector().add( new Tuple( value ) );

    Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();

    while( iterator.hasNext() )
      {
      TupleEntry arguments = iterator.next(); // must be called

      if( exepectedSize != -1 && arguments.size() != exepectedSize )
        throw new RuntimeException( "arguments wrong size" );

      bufferCall.getOutputCollector().add( new Tuple( value ) );
      }

    if( insertFooter )
      bufferCall.getOutputCollector().add( new Tuple( value ) );

    }
  }
