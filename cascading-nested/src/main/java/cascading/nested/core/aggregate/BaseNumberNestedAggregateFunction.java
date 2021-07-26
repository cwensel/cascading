/*
 * Copyright (c) 2016-2021 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.nested.core.aggregate;

import java.util.Objects;
import java.util.function.Supplier;

import cascading.nested.core.NestedAggregateFunction;
import cascading.operation.SerPredicate;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;

/**
 * Class BaseNumberNestedAggregateFunction is the base class used to create number oriented aggregation operations.
 * <p>
 * Subclasses can optimize based on an expected primitive type while still honoring the {@code Long.class} and
 * {@code Long.TYPE} semantics around {@code null} or {@code 0} empty values.
 * <p>
 * Note that subclasses can also be independent of the {@code Node} type (JSON etc). All Node specific operations
 * are passed back to the {@link cascading.nested.core.NestedCoercibleType} instance.
 *
 * @see SumLongNestedAggregateFunction
 * @see SumDoubleNestedAggregateFunction
 * @see AverageDoubleNestedAggregateFunction
 */
public abstract class BaseNumberNestedAggregateFunction<Node, Type, Context extends BaseNumberNestedAggregateFunction.BaseContext<Type, Node>> implements NestedAggregateFunction<Node, Context>
  {
  public abstract static class BaseContext<Type, Node>
    {
    final Tuple results;
    final SerPredicate<Type> discardValue;
    final Supplier<Tuple> complete;
    boolean allValuesDiscarded = true;

    public BaseContext( BaseNumberNestedAggregateFunction<Node, Type, BaseContext<Type, Node>> aggregateFunction )
      {
      results = createResultTuple( aggregateFunction );

      if( aggregateFunction.discardNullValues() )
        discardValue = Objects::isNull;
      else
        discardValue = v -> false;

      if( aggregateFunction.returnNullForEmpty() )
        complete = this::nullIfDiscard;
      else
        complete = this::valueIfDiscard;
      }

    protected Tuple createResultTuple( BaseNumberNestedAggregateFunction<Node, Type, BaseContext<Type, Node>> aggregateFunction )
      {
      return Tuple.size( aggregateFunction.getFieldDeclaration().size() );
      }

    protected Tuple valueIfDiscard()
      {
      completeAggregateValue( results );

      return results;
      }

    protected Tuple nullIfDiscard()
      {
      if( allValuesDiscarded )
        results.setAllTo( null );
      else
        completeAggregateValue( results );

      return results;
      }

    public void aggregate( CoercibleType<Node> coercibleType, Node node, Class<Type> aggregateType )
      {
      // if node is missing, always return null
      if( node == null )
        return;

      Type value = coercibleType.coerce( node, aggregateType );

      addAggregateValue( value );
      }

    protected void addAggregateValue( Type value )
      {
      if( discardValue.test( value ) )
        return;

      allValuesDiscarded = false;

      aggregateFilteredValue( value );
      }

    protected abstract void aggregateFilteredValue( Type value );

    public Tuple complete()
      {
      return complete.get();
      }

    protected abstract void completeAggregateValue( Tuple results );

    public void reset()
      {
      this.allValuesDiscarded = true;
      }
    }

  protected Fields fieldDeclaration;
  protected Class<Type> aggregateType;

  protected BaseNumberNestedAggregateFunction()
    {
    }

  protected BaseNumberNestedAggregateFunction( Fields fieldDeclaration, Class<Type> defaultType )
    {
    if( !fieldDeclaration.hasTypes() )
      fieldDeclaration = fieldDeclaration.applyTypeToAll( defaultType );

    this.fieldDeclaration = fieldDeclaration;
    this.aggregateType = fieldDeclaration.getTypeClass( 0 ); // if coercible type, returns canonical type

    if( Coercions.asNonPrimitive( this.aggregateType ) != Coercions.asNonPrimitive( defaultType ) )
      throw new IllegalArgumentException( "fieldDeclaration must declare either " + defaultType.getSimpleName() + " object or primitive type" );
    }

  @Override
  public Fields getFieldDeclaration()
    {
    return fieldDeclaration;
    }

  protected boolean returnNullForEmpty()
    {
    return !aggregateType.isPrimitive();
    }

  protected boolean discardNullValues()
    {
    return !aggregateType.isPrimitive();
    }

  @Override
  public void aggregate( Context context, CoercibleType<Node> coercibleType, Node node )
    {
    context.aggregate( coercibleType, node, aggregateType );
    }

  @Override
  public Tuple complete( Context context )
    {
    return context.complete();
    }

  @Override
  public Context resetContext( Context context )
    {
    context.reset();

    return context;
    }
  }
