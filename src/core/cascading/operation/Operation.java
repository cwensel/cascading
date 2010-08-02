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

package cascading.operation;

import cascading.flow.FlowProcess;
import cascading.tuple.Fields;

/**
 * Interface Operation is the base interface for all functions applied to {@link cascading.tuple.Tuple} streams.
 * <p/>
 * Specifically {@link Function}, {@link Filter}, {@link Aggregator}, {@link Buffer}, and {@link Assertion}.
 * <p/>
 * Use {@link BaseOperation} for a convenient way to create new Operation types.
 *
 * @see cascading.operation.BaseOperation
 * @see Function
 * @see Filter
 * @see Aggregator
 * @see Buffer
 * @see Assertion
 */
public interface Operation<C>
  {
  /** Field ANY denotes that a given Operation will take any number of argument values */
  int ANY = Integer.MAX_VALUE;

  /**
   * The prepare method is called immediately before the current Operation instance is put into play processing Tuples.
   * This method should initialize any resources that can be shutdown or released in the
   * {@link #cleanup(cascading.flow.FlowProcess, OperationCall)} method.
   * <p/>
   * This method may be called more than once during the life of this instance. But it will never be called multiple times
   * without a cleanup invocation immediately before subsequent invocations.
   * <p/>
   * If the Flow this Operation instance belongs will execute on a remote cluster, this method will be called
   * cluster side, not client side.
   *
   * @param flowProcess
   * @param operationCall
   */
  void prepare( FlowProcess flowProcess, OperationCall<C> operationCall );

  /**
   * The cleanup method is called immediately after the current Operation instance is taken out of play processing Tuples.
   * This method should shutdown any resources created or initialized during the
   * {@link #prepare(cascading.flow.FlowProcess, OperationCall)} method.
   * <p/>
   * This method may be called more than once during the life of this instance. But it will never be called multiple times
   * without a prepare invocation before.
   * <p/>
   * If the Flow this Operation instance belongs will execute on a remote cluster, this method will be called
   * cluster side, not client side.
   *
   * @param flowProcess
   * @param operationCall
   */
  void cleanup( FlowProcess flowProcess, OperationCall<C> operationCall );

  /**
   * Returns the fields created by this Operation instance. If this instance is a {@link Filter}, it should always
   * return {@link Fields#ALL}.
   *
   * @return
   */
  Fields getFieldDeclaration();

  /**
   * The minimum number of arguments this Operation expects from the calling {@link cascading.pipe.Each} or
   * {@link cascading.pipe.Every} Operator.
   * <p/>
   * Operations should be willing to receive more arguments than expected, but should ignore them if they are unused,
   * instead of failing.
   *
   * @return an int
   */
  int getNumArgs();

  /**
   * Returns {@code true} if this Operation instance can safely execute on the same 'record' multiple
   * times, {@code false} otherwise.
   * <p/>
   * That is, this Operation is safe if it has no side-effects, or if it does, they are idempotent.
   * <p/>
   * If seeing the same 'record' more than once can cause errors (internally or externally),
   * this method must return {@code false}.
   *
   * @return a boolean
   */
  boolean isSafe();
  }