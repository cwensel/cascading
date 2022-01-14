/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

import cascading.nested.core.NestedAggregate;
import cascading.operation.SerPredicate;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.coerce.Coercions;
import cascading.tuple.type.CoercibleType;
import cascading.tuple.type.CoercionFrom;

/**
 * Class BaseNumberNestedAggregate is the base class used to create number oriented aggregation operations.
 * <p>
 * Subclasses can optimize based on an expected primitive type while still honoring the {@code Long.class} and
 * {@code Long.TYPE} semantics around {@code null} or {@code 0} empty values.
 * <p>
 * Note that subclasses can also be independent of the {@code Node} type (JSON etc). All Node specific operations
 * are passed back to the {@link cascading.nested.core.NestedCoercibleType} instance.
 *
 * @see SumLongNestedAggregate
 * @see SumDoubleNestedAggregate
 * @see AverageDoubleNestedAggregate
 */
public abstract class BaseNumberNestedAggregate<Node, Type, Context extends BaseNumberNestedAggregate.BaseContext<Type, Node>> implements NestedAggregate<Node, Context>
  {
  public abstract static class BaseContext<Type, Node>
    {
    protected final CoercibleType<Node> coercibleType;
    protected final CoercionFrom<Node, Type> to;
    protected final Tuple results;
    protected final SerPredicate<Type> discardValue;
    protected final Supplier<Tuple> complete;
    protected boolean allValuesDiscarded = true;

    public BaseContext( BaseNumberNestedAggregate<Node, Type, BaseContext<Type, Node>> aggregateFunction, CoercibleType<Node> coercibleType )
      {
      this.coercibleType = coercibleType;
      this.to = coercibleType.to( aggregateFunction.aggregateType );

      this.results = createResultTuple( aggregateFunction );

      if( aggregateFunction.discardNullValues() )
        this.discardValue = Objects::isNull;
      else
        this.discardValue = v -> false;

      if( aggregateFunction.returnNullForEmpty() )
        this.complete = this::nullIfDiscard;
      else
        this.complete = this::valueIfDiscard;
      }

    protected Tuple createResultTuple( BaseNumberNestedAggregate<Node, Type, BaseContext<Type, Node>> aggregateFunction )
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

    public void aggregate( Node node )
      {
      // if node is missing, always return null
      if( node == null )
        return;

      Type value = coerceFrom( node );

      addAggregateValue( value );
      }

    protected Type coerceFrom( Node node )
      {
      return to.coerce( node );
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

  protected BaseNumberNestedAggregate()
    {
    }

  protected BaseNumberNestedAggregate( Fields fieldDeclaration, Class<Type> defaultType )
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
  public void aggregate( Context context, Node node )
    {
    context.aggregate( node );
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
