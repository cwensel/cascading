/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
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
public interface Operation<Context>
  {
  /** Field ANY denotes that a given Operation will take any number of argument values */
  int ANY = Integer.MAX_VALUE;

  /**
   * The prepare method is called immediately before the current Operation instance is put into play processing Tuples.
   * This method should initialize any resources that can be shutdown or released in the
   * {@link #cleanup(cascading.flow.FlowProcess, OperationCall)} method.
   * <p/>
   * Any resources created should be stored in the {@code Context}, not as instance fields on the class.
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
  void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall );

  /**
   * The flush method is called when an Operation that is caching values must empty the cache. It is called before
   * {@link #cleanup(cascading.flow.FlowProcess, OperationCall)} is invoked.
   * <p/>
   * It is safe to cast the {@link cascading.operation.OperationCall} to a {@link FunctionCall}, or equivalent, and
   * get its {@link cascading.operation.FunctionCall#getOutputCollector()}.
   *
   * @param flowProcess
   * @param operationCall
   */
  void flush( FlowProcess flowProcess, OperationCall<Context> operationCall );

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
  void cleanup( FlowProcess flowProcess, OperationCall<Context> operationCall );

  /**
   * Returns the fields created by this Operation instance. If this instance is a {@link Filter}, it should always
   * return {@link Fields#ALL}.
   *
   * @return a Fields instance
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