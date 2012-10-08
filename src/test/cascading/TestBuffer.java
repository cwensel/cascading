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
  private int expectedSize = -1;
  private boolean insertHeader;
  private boolean insertFooter;
  private Comparable value;

  public TestBuffer( Fields fieldDeclaration, int expectedSize, boolean insertHeader, boolean insertFooter, String value )
    {
    super( fieldDeclaration );
    this.expectedSize = expectedSize;
    this.insertHeader = insertHeader;
    this.insertFooter = insertFooter;
    this.value = value;
    }

  public TestBuffer( Fields fieldDeclaration, int expectedSize, boolean insertHeader, String value )
    {
    super( fieldDeclaration );
    this.expectedSize = expectedSize;
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

  public TestBuffer( Fields fieldDeclaration )
    {
    super( fieldDeclaration );
    }

  public void operate( FlowProcess flowProcess, BufferCall bufferCall )
    {
    if( insertHeader )
      bufferCall.getOutputCollector().add( new Tuple( value ) );

    Iterator<TupleEntry> iterator = bufferCall.getArgumentsIterator();

    while( iterator.hasNext() )
      {
      TupleEntry arguments = iterator.next(); // must be called

      if( expectedSize != -1 && arguments.size() != expectedSize )
        throw new RuntimeException( "arguments wrong size" );

      if( value != null )
        bufferCall.getOutputCollector().add( new Tuple( value ) );
      else
        bufferCall.getOutputCollector().add( arguments ); // copy
      }

    if( insertFooter )
      bufferCall.getOutputCollector().add( new Tuple( value ) );

    iterator.hasNext(); // regression
    }
  }
