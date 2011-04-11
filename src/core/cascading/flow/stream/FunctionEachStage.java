/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stream;

import java.io.IOException;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.pipe.Each;
import cascading.pipe.OperatorException;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;
import cascading.tuple.Tuples;

/**
 *
 */
public class FunctionEachStage extends EachStage
  {
  private Function function;

  public FunctionEachStage( FlowProcess flowProcess, Each each )
    {
    super( flowProcess, each );
    }

  @Override
  public void initialize()
    {
    super.initialize();

    function = each.getFunction();

    operationCall.setArguments( argumentsEntry );

    operationCall.setOutputCollector( new TupleEntryCollector( outgoingScopes.get( 0 ).getDeclaredFields() )
    {
    @Override
    protected void collect( TupleEntry input ) throws IOException
      {
      Tuple outgoing = each.makeResult( outgoingSelector, incomingEntry, remainderFields, input, input.getTuple() );

      outgoingEntry.setTuple( outgoing );

      try
        {
        next.receive( FunctionEachStage.this, outgoingEntry );
        }
      finally
        {
        Tuples.asModifiable( outgoing );
        }
      }
    } );
    }

  @Override
  public void receive( Duct previous, TupleEntry incomingEntry )
    {
    this.incomingEntry = incomingEntry;

    argumentsEntry.setTuple( incomingEntry.selectTuple( argumentsSelector ) );

    try
      {
      function.operate( flowProcess, operationCall ); // adds results to collector
      }
    catch( CascadingException exception )
      {
      handleException( exception, incomingEntry );
      }
    catch( Throwable throwable )
      {
      handleException( new OperatorException( each, "operator Each failed executing operation", throwable ), incomingEntry );
      }
    }
  }
