/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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

package cascading.operation;

import java.util.Iterator;

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;
import cascading.tuple.TupleEntryCollector;

/**
 * Class OperationCall is the common base class for {@link FunctionCall}, {@link FilterCall},
 * {@link AggregatorCall}, {@link ValueAssertionCall}, and {@link GroupAssertionCall}.
 */
public class ConcreteCall<C> implements FunctionCall<C>, FilterCall<C>, AggregatorCall<C>, BufferCall<C>, ValueAssertionCall<C>, GroupAssertionCall<C>
  {
  /** Field context */
  private C context;
  /** Field group */
  private TupleEntry group;
  /** Field argumentFields */
  private Fields argumentFields;
  /** Field arguments */
  private TupleEntry arguments;
  /** Field argumentsIterator */
  private Iterator<TupleEntry> argumentsIterator;
  /** Field declaredFields */
  private Fields declaredFields;
  /** Field outputCollector */
  private TupleEntryCollector outputCollector;
  /** Field retainValues */
  private boolean retainValues = false;

  /** Constructor OperationCall creates a new OperationCall instance. */
  public ConcreteCall()
    {
    }

  /**
   * Constructor ConcreteCall creates a new ConcreteCall instance.
   *
   * @param argumentFields of type Fields
   */
  public ConcreteCall( Fields argumentFields )
    {
    this.argumentFields = argumentFields;
    }

  /**
   * Constructor ConcreteCall creates a new ConcreteCall instance.
   *
   * @param argumentFields of type Fields
   * @param declaredFields of type Fields
   */
  public ConcreteCall( Fields argumentFields, Fields declaredFields )
    {
    this.argumentFields = argumentFields;
    this.declaredFields = declaredFields;
    }

  /**
   * Constructor OperationCall creates a new OperationCall instance.
   *
   * @param arguments       of type TupleEntry
   * @param outputCollector of type TupleCollector
   */
  public ConcreteCall( TupleEntry arguments, TupleEntryCollector outputCollector )
    {
    this.arguments = arguments;
    this.outputCollector = outputCollector;
    }

  /** @see AggregatorCall#getContext() */
  public C getContext()
    {
    return context;
    }

  public void setContext( C context )
    {
    this.context = context;
    }

  /** @see AggregatorCall#getGroup() */
  public TupleEntry getGroup()
    {
    return group;
    }

  public void setGroup( TupleEntry group )
    {
    this.group = group;
    }

  public Fields getArgumentFields()
    {
    return argumentFields;
    }

  /** @see BufferCall#getArgumentsIterator() */
  public Iterator<TupleEntry> getArgumentsIterator()
    {
    return argumentsIterator;
    }

  public void setArgumentsIterator( Iterator<TupleEntry> argumentsIterator )
    {
    this.argumentsIterator = argumentsIterator;
    }

  /** @see FunctionCall#getArguments() */
  public TupleEntry getArguments()
    {
    return arguments;
    }

  public void setArguments( TupleEntry arguments )
    {
    this.arguments = arguments;
    }

  public Fields getDeclaredFields()
    {
    return declaredFields;
    }

  /** @see FunctionCall#getOutputCollector() */
  public TupleEntryCollector getOutputCollector()
    {
    return outputCollector;
    }

  public void setOutputCollector( TupleEntryCollector outputCollector )
    {
    this.outputCollector = outputCollector;
    }

  @Override
  public void setRetainValues( boolean retainValues )
    {
    this.retainValues = retainValues;
    }

  @Override
  public boolean isRetainValues()
    {
    return retainValues;
    }
  }