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

import java.beans.ConstructorProperties;
import java.io.Serializable;

import cascading.nested.core.NestedAggregate;
import cascading.operation.SerFunction;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.type.CoercibleType;

/**
 * Class SimpleNestedAggregate provides a simple extensible implementation of {@link NestedAggregate}.
 *
 * @param <Node>
 */
public class SimpleNestedAggregate<Node> implements NestedAggregate<Node, SimpleNestedAggregate.AggregateContext<Node>>
  {
  public interface AggregateContext<Node> extends Serializable
    {
    void aggregate( Node node );

    Tuple complete();

    void reset();
    }

  protected Fields fieldDeclaration;
  protected SerFunction<CoercibleType<Node>, AggregateContext<Node>> factory;

  /**
   * Create a new instance of SimpleNestedAggregate.
   * <p>
   * The factory parameter provides new instances of {@link AggregateContext}.
   *
   * @param fieldDeclaration the fields this aggregate returns
   * @param factory          a supplier for new AggregateContext instances
   */
  @ConstructorProperties({"fieldDeclaration", "factory"})
  public SimpleNestedAggregate( Fields fieldDeclaration, SerFunction<CoercibleType<Node>, SimpleNestedAggregate.AggregateContext<Node>> factory )
    {
    this.fieldDeclaration = fieldDeclaration;
    this.factory = factory;
    }

  /**
   * Create a new instance of SimpleNestedAggregate without a factory
   * <p>
   * The factory parameter provides new instances of {@link AggregateContext}.
   *
   * @param fieldDeclaration the fields this aggregate returns
   */
  @ConstructorProperties({"fieldDeclaration"})
  protected SimpleNestedAggregate( Fields fieldDeclaration )
    {
    this.fieldDeclaration = fieldDeclaration;
    }

  /**
   * Create a new instance of SimpleNestedAggregate without a factory
   * <p>
   * The factory parameter provides new instances of {@link AggregateContext}.
   */
  protected SimpleNestedAggregate()
    {
    }

  protected void setFactory( SerFunction<CoercibleType<Node>, AggregateContext<Node>> factory )
    {
    this.factory = factory;
    }

  @Override
  public Fields getFieldDeclaration()
    {
    return fieldDeclaration;
    }

  protected SimpleNestedAggregate<Node> setFieldDeclaration( Fields fieldDeclaration )
    {
    this.fieldDeclaration = fieldDeclaration;
    return this;
    }

  @Override
  public AggregateContext<Node> createContext( CoercibleType<Node> nestedCoercibleType )
    {
    if( factory == null )
      throw new IllegalStateException( "factory is required" );

    return factory.apply( nestedCoercibleType );
    }

  @Override
  public void aggregate( AggregateContext<Node> context, Node node )
    {
    context.aggregate( node );
    }

  @Override
  public Tuple complete( AggregateContext<Node> context )
    {
    return context.complete();
    }

  @Override
  public AggregateContext<Node> resetContext( AggregateContext<Node> context )
    {
    context.reset();

    return context;
    }
  }
