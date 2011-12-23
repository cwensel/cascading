/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.pipe;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import cascading.flow.Scope;
import cascading.tuple.Fields;

/**
 * Splice is the base class for all merge, union, and join Pipes.
 *
 * @see Merge
 */
public class Splice extends Pipe
  {
  /** Field pipes */
  private final List<Pipe> pipes = new ArrayList<Pipe>();

  private String spliceName;

  /** Field pipePos */
  private transient Map<String, Integer> pipePos;

  Splice( String name, Pipe[] pipes )
    {
    this.spliceName = name;

    if( pipes == null || pipes.length < 2 )
      throw new IllegalArgumentException( "should be more than one pipe to splice" );

    for( Pipe pipe : pipes )
      addPipe( pipe );
    }

  @Override
  public String getName()
    {
    if( spliceName != null )
      return spliceName;

    StringBuffer buffer = new StringBuffer();

    for( Pipe pipe : pipes )
      {
      if( buffer.length() != 0 )
        {
        if( isMerge() )
          buffer.append( "+" );
        else
          buffer.append( "*" ); // more semantically correct
        }

      buffer.append( pipe.getName() );
      }

    spliceName = buffer.toString();

    return spliceName;
    }

  public boolean isMerge()
    {
    return true;
    }

  @Override
  public Pipe[] getPrevious()
    {
    return pipes.toArray( new Pipe[ pipes.size() ] );
    }

  private void addPipe( Pipe pipe )
    {
    if( pipe.getName() == null )
      throw new IllegalArgumentException( "each input pipe must have a name" );

    pipes.add( pipe ); // allow same pipe
    }

  public synchronized Map<String, Integer> getPipePos()
    {
    if( pipePos != null )
      return pipePos;

    pipePos = new HashMap<String, Integer>();

    int pos = 0;
    for( Object pipe : pipes )
      pipePos.put( ( (Pipe) pipe ).getName(), pos++ );

    return pipePos;
    }

  @Override
  public Scope outgoingScopeFor( Set<Scope> incomingScopes )
    {
    if( !isMerge() )
      throw new IllegalStateException( "type not recognized" );

    Fields commonFields = null;

    for( Scope incomingScope : incomingScopes )
      {
      Fields fields = resolveFields( incomingScope );

      if( commonFields == null )
        commonFields = fields;
      else if( !commonFields.equals( fields ) )
        throw new OperatorException( this, "merged streams must declare the same field names, expected: " + commonFields.printVerbose() + " found: " + fields.print() );
      }

    return new Scope( getName(), commonFields, isMerge() );
    }

  @Override
  public Fields resolveFields( Scope scope )
    {
    if( scope.isEvery() )
      return scope.getOutGroupingFields();
    else
      return scope.getOutValuesFields();
    }
  }
