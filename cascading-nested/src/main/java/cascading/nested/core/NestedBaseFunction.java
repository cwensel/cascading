/*
 * Copyright (c) 2016-2020 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.nested.core;

import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.SerPredicate;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.util.Pair;
import heretical.pointer.path.NestedPointerCompiler;
import heretical.pointer.path.Pointer;

/**
 *
 */
public abstract class NestedBaseFunction<Node, Result> extends NestedBaseOperation<Node, Result, NestedBaseFunction.Context> implements Function<NestedBaseFunction.Context>
  {
  protected class Context
    {
    public Tuple result;
    public Map<Fields, Pair<SerPredicate<?>, Pointer<Node>>> pointers;

    public Context( Map<Fields, Pair<SerPredicate<?>, Pointer<Node>>> pointers, Tuple result )
      {
      this.result = result;
      this.pointers = pointers;
      }
    }

  protected String rootPointer = "";
  protected SerPredicate<?> defaultValueFilter = ( v ) -> true;
  protected Map<Fields, Pointer<Node>> pointers = new LinkedHashMap<>();

  public NestedBaseFunction( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration )
    {
    this( nestedCoercibleType, fieldDeclaration, Collections.emptyMap() );
    }

  public NestedBaseFunction( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration, SerPredicate<?> defaultValueFilter )
    {
    this( nestedCoercibleType, fieldDeclaration, defaultValueFilter, Collections.emptyMap() );
    }

  public NestedBaseFunction( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration, String rootPointer )
    {
    this( nestedCoercibleType, fieldDeclaration, Collections.emptyMap() );

    if( rootPointer != null )
      this.rootPointer = rootPointer;
    }

  public NestedBaseFunction( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration, String rootPointer, SerPredicate<?> defaultValueFilter )
    {
    this( nestedCoercibleType, fieldDeclaration, defaultValueFilter, Collections.emptyMap() );

    if( rootPointer != null )
      this.rootPointer = rootPointer;
    }

  public NestedBaseFunction( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration, Map<Fields, String> pointerMap )
    {
    this( nestedCoercibleType, fieldDeclaration, null, pointerMap );
    }

  public NestedBaseFunction( NestedCoercibleType<Node, Result> nestedCoercibleType, Fields fieldDeclaration, SerPredicate<?> defaultValueFilter, Map<Fields, String> pointerMap )
    {
    super( nestedCoercibleType, fieldDeclaration );

    if( defaultValueFilter != null )
      this.defaultValueFilter = defaultValueFilter;

    if( pointerMap != null && !pointerMap.isEmpty() )
      {
      NestedPointerCompiler<Node, Result> compiler = getNestedPointerCompiler();

      for( Map.Entry<Fields, String> entry : pointerMap.entrySet() )
        this.pointers.put( entry.getKey(), compiler.compile( entry.getValue() ) );
      }
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<NestedBaseFunction.Context> operationCall )
    {
    Map<Fields, Pair<SerPredicate<?>, Pointer<Node>>> resolvedPointers = new LinkedHashMap<>();
    Fields argumentFields = operationCall.getArgumentFields();

    for( Map.Entry<Fields, Pointer<Node>> entry : this.pointers.entrySet() )
      resolvedPointers.put( argumentFields.select( entry.getKey() ), new Pair<>( defaultValueFilter, entry.getValue() ) );

    if( resolvedPointers.isEmpty() ) // use resolved argument fields
      {
      NestedPointerCompiler<Node, Result> compiler = getNestedPointerCompiler();

      for( Iterator<Fields> iterator = argumentFields.fieldsIterator(); iterator.hasNext(); )
        {
        Fields argument = iterator.next();

        Pointer<Node> pointer = compiler.compile( rootPointer + "/" + argument.get( 0 ).toString() );
        resolvedPointers.put( argument, new Pair<>( defaultValueFilter, pointer ) );
        }
      }

    operationCall.setContext( new Context( resolvedPointers, Tuple.size( 1 ) ) );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<NestedBaseFunction.Context> functionCall )
    {
    Node node = getNode( functionCall.getArguments() );

    Set<Map.Entry<Fields, Pair<SerPredicate<?>, Pointer<Node>>>> entries = functionCall.getContext().pointers.entrySet();

    for( Map.Entry<Fields, Pair<SerPredicate<?>, Pointer<Node>>> entry : entries )
      {
      Fields key = entry.getKey();
      SerPredicate<Object> predicate = (SerPredicate<Object>) entry.getValue().getLhs();
      Pointer<Node> pointer = entry.getValue().getRhs();

      Object argumentValue = functionCall.getArguments().getObject( key );

      if( predicate.test( argumentValue ) )
        {
        Node result = getLiteralNode( argumentValue );

        pointer.set( node, result );
        }
      }

    functionCall.getContext().result.set( 0, node );

    functionCall.getOutputCollector().add( functionCall.getContext().result );
    }

  protected abstract Node getNode( TupleEntry arguments );
  }
