/*
 * Copyright (c) 2007-2010 Concurrent, Inc. All Rights Reserved.
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

package cascading.flow.stack;

import java.io.IOException;

import cascading.CascadingException;
import cascading.flow.FlowElement;
import cascading.flow.FlowException;
import cascading.flow.FlowProcess;
import cascading.flow.Scope;
import cascading.flow.StepCounters;
import cascading.tap.Tap;
import cascading.tuple.Tuple;

/**
 *
 */
class SourceMapperStackElement extends MapperStackElement
  {
  private final Tap source;

  public SourceMapperStackElement( FlowProcess flowProcess, Scope incomingScope, String trapName, Tap trap, Tap source ) throws IOException
    {
    super( flowProcess, incomingScope, trapName, trap );
    this.source = source;
    }

  public SourceMapperStackElement( FlowProcess flowProcess, Scope incomingScope, Tap source ) throws IOException
    {
    super( flowProcess, incomingScope, null, null );
    this.source = source;
    }

  protected FlowElement getFlowElement()
    {
    return source;
    }

  @Override
  public Tuple source( Object key, Object value )
    {
    Tuple result = null;

    try
      {
      result = operateSource( key, value );
      }
    catch( Exception exception )
      {
      handleException( exception, getTupleEntry( new Tuple( key, value ) ) );
      }

    return result;
    }

  private Tuple operateSource( Object key, Object value )
    {
    try
      {
      Tuple tuple = source.source( key, value );

      getFlowProcess().increment( StepCounters.Tuples_Read, 1 );

      return tuple;
      }
    catch( OutOfMemoryError error )
      {
      throw new StackException( "out of memory, try increasing task memory allocation", error );
      }
    catch( Throwable throwable )
      {
      if( throwable instanceof CascadingException )
        throw (CascadingException) throwable;

      throw new FlowException( "internal error: " + key.toString() + " " + value.toString(), throwable );
      }
    }

  public void prepare()
    {
    // do nothing
    }

  public void cleanup()
    {
    // do nothing
    }
  }