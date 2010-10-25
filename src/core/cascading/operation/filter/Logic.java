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

package cascading.operation.filter;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.OperationCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Class Logic is the base class for logical {@link Filter} operations.
 *
 * @see And
 * @see Or
 * @see Xor
 */
public abstract class Logic extends BaseOperation<Logic.Context> implements Filter<Logic.Context>
  {
  /** Field fields */
  protected final Fields[] argumentSelectors;
  /** Field filters */
  protected final Filter[] filters;

  private static Filter[] filters( Filter... filters )
    {
    return filters;
    }

  public class Context
    {
    TupleEntry[] argumentEntries;
    Object[] contexts;
    }

  @ConstructorProperties({"filters"})
  protected Logic( Filter... filters )
    {
    this.filters = filters;

    if( filters == null )
      throw new IllegalArgumentException( "given filters array must not be null" );

    this.argumentSelectors = new Fields[filters.length];
    Arrays.fill( this.argumentSelectors, Fields.ALL );

    verify();

    this.numArgs = getFieldsSize();
    }

  @ConstructorProperties({"lhsArgumentsSelector", "lhsFilter", "rhsArgumentSelector", "rhsFilter"})
  protected Logic( Fields lhsArgumentSelector, Filter lhsFilter, Fields rhsArgumentSelector, Filter rhsFilter )
    {
    this( Fields.fields( lhsArgumentSelector, rhsArgumentSelector ), filters( lhsFilter, rhsFilter ) );
    }

  @ConstructorProperties({"argumentSelectors", "filters"})
  protected Logic( Fields[] argumentSelectors, Filter[] filters )
    {
    this.argumentSelectors = argumentSelectors;
    this.filters = filters;

    verify();

    this.numArgs = getFieldsSize();
    }

  protected void verify()
    {
    if( argumentSelectors == null )
      throw new IllegalArgumentException( "given argumentSelectors array must not be null" );

    if( filters == null )
      throw new IllegalArgumentException( "given filters array must not be null" );

    for( Fields field : argumentSelectors )
      {
      if( field == null )
        throw new IllegalArgumentException( "given argumentSelectors must not be null" );

      if( !field.isAll() && !field.isDefined() )
        throw new IllegalArgumentException( "given argumentSelectors must be ALL or 'defined' selectors, got: " + field.print() );
      }

    for( Filter filter : filters )
      {
      if( filter == null )
        throw new IllegalArgumentException( "given filters must not be null" );
      }
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall operationCall )
    {
    Context context = new Context();

    context.argumentEntries = getArgumentEntries();
    context.contexts = new Object[filters.length];

    for( int i = 0; i < filters.length; i++ )
      {
      Filter filter = filters[ i ];

      filter.prepare( flowProcess, operationCall );

      context.contexts[ i ] = operationCall.getContext();

      operationCall.setContext( null );
      }

    operationCall.setContext( context );
    }

  @Override
  public void cleanup( FlowProcess flowProcess, OperationCall operationCall )
    {
    Context context = (Context) operationCall.getContext();
    Object[] contexts = context.contexts;

    for( int i = 0; i < filters.length; i++ )
      {
      Filter filter = filters[ i ];

      operationCall.setContext( contexts[ i ] );

      filter.cleanup( flowProcess, operationCall );
      }

    operationCall.setContext( null );
    }

  protected int getFieldsSize()
    {
    Set<Comparable> pos = new HashSet<Comparable>();

    for( Fields field : argumentSelectors )
      {
      if( field.isSubstitution() ) // will be tested to be ALL in verify
        return ANY;

      for( int i = 0; i < field.size(); i++ )
        pos.add( field.get( i ) );
      }

    return pos.size();
    }

  private final TupleEntry[] getArgumentEntries()
    {
    TupleEntry[] argumentEntries = new TupleEntry[argumentSelectors.length];

    for( int i = 0; i < argumentSelectors.length; i++ )
      {
      Fields argumentSelector = argumentSelectors[ i ];
      argumentEntries[ i ] = new TupleEntry( Fields.asDeclaration( argumentSelector ) );
      }

    return argumentEntries;
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof Logic ) )
      return false;
    if( !super.equals( object ) )
      return false;

    Logic logic = (Logic) object;

    if( !Arrays.equals( argumentSelectors, logic.argumentSelectors ) )
      return false;
    if( !Arrays.equals( filters, logic.filters ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( argumentSelectors != null ? Arrays.hashCode( argumentSelectors ) : 0 );
    result = 31 * result + ( filters != null ? Arrays.hashCode( filters ) : 0 );
    return result;
    }
  }
