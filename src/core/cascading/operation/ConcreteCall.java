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
  /** Field outputCollector */
  private TupleEntryCollector outputCollector;

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

  /** @see FunctionCall#getOutputCollector() */
  public TupleEntryCollector getOutputCollector()
    {
    return outputCollector;
    }

  public void setOutputCollector( TupleEntryCollector outputCollector )
    {
    this.outputCollector = outputCollector;
    }

  }