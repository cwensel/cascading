/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.nested.json;

import java.beans.ConstructorProperties;
import java.util.Collections;
import java.util.Map;
import java.util.function.Predicate;

import cascading.nested.core.NestedCreateFunction;
import cascading.tuple.Fields;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

/**
 * Class JSONCreateFunction provides for the ability to simply set multiple tuple values onto a new JSON object.
 * <p>
 * If a {@link Predicate} is supplied, it will be tested on the current value, if {@code true}, the value
 * will be set on the final JSON object. This is useful for prevent null values from being set, artificially
 * increasing the size of the final object.
 *
 * @see NestedCreateFunction for more details.
 */
public class JSONCreateFunction extends NestedCreateFunction<JsonNode, ArrayNode>
  {
  /**
   * Creates a new JSONCreateFunction instance that will pivot all resolved arguments fields into
   * a new JSON object, where all the JSON object attributes are the argument field names.
   *
   * @param fieldDeclaration of Fields
   */
  @ConstructorProperties({"fieldDeclaration"})
  public JSONCreateFunction( Fields fieldDeclaration )
    {
    super( JSONCoercibleType.TYPE, fieldDeclaration );
    }

  /**
   * Creates a new JSONCreateFunction instance that will pivot all resolved arguments fields into
   * a new JSON object, where all the JSON object attributes are the argument field names.
   * <p>
   * The {@code rootPointer} values specifies the base name of the final pointer path. If {@code rootPointer} is
   * "/person" and and argument is passed with field name "fullName", the value will be placed in "/person/fullName".
   *
   * @param fieldDeclaration of Fields
   * @param rootPointer      of String
   */
  @ConstructorProperties({"fieldDeclaration", "rootPointer"})
  public JSONCreateFunction( Fields fieldDeclaration, String rootPointer )
    {
    super( JSONCoercibleType.TYPE, fieldDeclaration, rootPointer );
    }

  /**
   * Creates a new JSONCreateFunction instance that will pivot all resolved arguments fields into
   * a new JSON object, where all the JSON object attributes are the argument field names.
   *
   * @param coercibleType    of JSONCoercibleType
   * @param fieldDeclaration of Fields
   */
  @ConstructorProperties({"coercibleType", "fieldDeclaration"})
  public JSONCreateFunction( JSONCoercibleType coercibleType, Fields fieldDeclaration )
    {
    super( coercibleType, fieldDeclaration );
    }

  /**
   * Creates a new JSONCreateFunction instance that will pivot all resolved arguments fields into
   * a new JSON object, where all the JSON object attributes are the argument field names.
   * <p>
   * The {@code rootPointer} values specifies the base name of the final pointer path. If {@code rootPointer} is
   * "/person" and and argument is passed with field name "fullName", the value will be placed in "/person/fullName".
   *
   * @param coercibleType    of JSONCoercibleType
   * @param fieldDeclaration of Fields
   * @param rootPointer      of String
   */
  @ConstructorProperties({"coercibleType", "fieldDeclaration", "rootPointer"})
  public JSONCreateFunction( JSONCoercibleType coercibleType, Fields fieldDeclaration, String rootPointer )
    {
    super( coercibleType, fieldDeclaration, rootPointer );
    }

  /**
   * Creates a new JSONCreateFunction instance that maps the given {@code fromFields} value to the location
   * declared by the {@code stringPointer}.
   *
   * @param fieldDeclaration of Fields
   * @param fromFields       of Fields
   * @param stringPointer    of String
   */
  @ConstructorProperties({"fieldDeclaration", "fromFields", "stringPointer"})
  public JSONCreateFunction( Fields fieldDeclaration, Fields fromFields, String stringPointer )
    {
    this( fieldDeclaration, Collections.singletonMap( fromFields, stringPointer ) );
    }

  /**
   * Creates a new JSONCreateFunction instance that maps all the Fields declared in the
   * {@code pointerMap} to the declared path pointers.
   *
   * @param fieldDeclaration of Fields
   * @param pointerMap       of Map
   */
  @ConstructorProperties({"fieldDeclaration", "pointerMap"})
  public JSONCreateFunction( Fields fieldDeclaration, Map<Fields, String> pointerMap )
    {
    super( JSONCoercibleType.TYPE, fieldDeclaration, pointerMap );
    }

  /**
   * Creates a new JSONCreateFunction instance that maps the given {@code fromFields} value to the location
   * declared by the {@code stringPointer}.
   *
   * @param coercibleType    of JSONCoercibleType
   * @param fieldDeclaration of Fields
   * @param fromFields       of Fields
   * @param stringPointer    of String
   */
  @ConstructorProperties({"coercibleType", "fieldDeclaration", "fromFields", "stringPointer"})
  public JSONCreateFunction( JSONCoercibleType coercibleType, Fields fieldDeclaration, Fields fromFields, String stringPointer )
    {
    this( coercibleType, fieldDeclaration, Collections.singletonMap( fromFields, stringPointer ) );
    }

  /**
   * Creates a new JSONCreateFunction instance that maps all the Fields declared in the
   * {@code pointerMap} to the declared path pointers.
   *
   * @param coercibleType    of JSONCoercibleType
   * @param fieldDeclaration of Fields
   * @param pointerMap       of Map
   */
  @ConstructorProperties({"coercibleType", "fieldDeclaration", "pointerMap"})
  public JSONCreateFunction( JSONCoercibleType coercibleType, Fields fieldDeclaration, Map<Fields, String> pointerMap )
    {
    super( coercibleType, fieldDeclaration, pointerMap );
    }

  /**
   * Creates a new JSONCreateFunction instance that will pivot all resolved arguments fields into
   * a new JSON object, where all the JSON object attributes are the argument field names.
   *
   * @param fieldDeclaration   of Fields
   * @param defaultValueFilter of Predicate
   */
  @ConstructorProperties({"fieldDeclaration", "defaultValueFilter"})
  public JSONCreateFunction( Fields fieldDeclaration, Predicate<?> defaultValueFilter )
    {
    super( JSONCoercibleType.TYPE, fieldDeclaration, defaultValueFilter );
    }

  /**
   * Creates a new JSONCreateFunction instance that will pivot all resolved arguments fields into
   * a new JSON object, where all the JSON object attributes are the argument field names.
   * <p>
   * The {@code rootPointer} values specifies the base name of the final pointer path. If {@code rootPointer} is
   * "/person" and and argument is passed with field name "fullName", the value will be placed in "/person/fullName".
   *
   * @param fieldDeclaration   of Fields
   * @param rootPointer        of String
   * @param defaultValueFilter of Predicate
   */
  @ConstructorProperties({"fieldDeclaration", "rootPointer", "defaultValueFilter"})
  public JSONCreateFunction( Fields fieldDeclaration, String rootPointer, Predicate<?> defaultValueFilter )
    {
    super( JSONCoercibleType.TYPE, fieldDeclaration, rootPointer, defaultValueFilter );
    }

  /**
   * Creates a new JSONCreateFunction instance that will pivot all resolved arguments fields into
   * a new JSON object, where all the JSON object attributes are the argument field names.
   *
   * @param coercibleType      of JSONCoercibleType
   * @param fieldDeclaration   of Fields
   * @param defaultValueFilter of Predicate
   */
  @ConstructorProperties({"coercibleType", "fieldDeclaration", "defaultValueFilter"})
  public JSONCreateFunction( JSONCoercibleType coercibleType, Fields fieldDeclaration, Predicate<?> defaultValueFilter )
    {
    super( coercibleType, fieldDeclaration, defaultValueFilter );
    }

  /**
   * Creates a new JSONCreateFunction instance that will pivot all resolved arguments fields into
   * a new JSON object, where all the JSON object attributes are the argument field names.
   * <p>
   * The {@code rootPointer} values specifies the base name of the final pointer path. If {@code rootPointer} is
   * "/person" and and argument is passed with field name "fullName", the value will be placed in "/person/fullName".
   *
   * @param coercibleType      of JSONCoercibleType
   * @param fieldDeclaration   of Fields
   * @param rootPointer        of String
   * @param defaultValueFilter of Predicate
   */
  @ConstructorProperties({"coercibleType", "fieldDeclaration", "rootPointer", "defaultValueFilter"})
  public JSONCreateFunction( JSONCoercibleType coercibleType, Fields fieldDeclaration, String rootPointer, Predicate<?> defaultValueFilter )
    {
    super( coercibleType, fieldDeclaration, rootPointer, defaultValueFilter );
    }

  /**
   * Creates a new JSONCreateFunction instance that maps the given {@code fromFields} value to the location
   * declared by the {@code stringPointer}.
   *
   * @param fieldDeclaration   of Fields
   * @param defaultValueFilter of Predicate
   * @param fromFields         of Fields
   * @param stringPointer      of String
   */
  @ConstructorProperties({"fieldDeclaration", "defaultValueFilter", "fromFields", "stringPointer"})
  public JSONCreateFunction( Fields fieldDeclaration, Predicate<?> defaultValueFilter, Fields fromFields, String stringPointer )
    {
    this( fieldDeclaration, defaultValueFilter, Collections.singletonMap( fromFields, stringPointer ) );
    }

  /**
   * Creates a new JSONCreateFunction instance that maps all the Fields declared in the
   * {@code pointerMap} to the declared path pointers.
   *
   * @param fieldDeclaration   of Fields
   * @param defaultValueFilter of Predicate
   * @param pointerMap         of Map
   */
  @ConstructorProperties({"fieldDeclaration", "defaultValueFilter", "pointerMap"})
  public JSONCreateFunction( Fields fieldDeclaration, Predicate<?> defaultValueFilter, Map<Fields, String> pointerMap )
    {
    super( JSONCoercibleType.TYPE, fieldDeclaration, defaultValueFilter, pointerMap );
    }

  /**
   * Creates a new JSONCreateFunction instance that maps the given {@code fromFields} value to the location
   * declared by the {@code stringPointer}.
   *
   * @param coercibleType      of JSONCoercibleType
   * @param fieldDeclaration   of Fields
   * @param defaultValueFilter of Predicate
   * @param fromFields         of Fields
   * @param stringPointer      of String
   */
  @ConstructorProperties({"coercibleType", "fieldDeclaration", "defaultValueFilter", "fromFields", "stringPointer"})
  public JSONCreateFunction( JSONCoercibleType coercibleType, Fields fieldDeclaration, Predicate<?> defaultValueFilter, Fields fromFields, String stringPointer )
    {
    this( coercibleType, fieldDeclaration, defaultValueFilter, Collections.singletonMap( fromFields, stringPointer ) );
    }

  /**
   * Creates a new JSONCreateFunction instance that maps all the Fields declared in the
   * {@code pointerMap} to the declared path pointers.
   *
   * @param coercibleType      of JSONCoercibleType
   * @param fieldDeclaration   of Fields
   * @param defaultValueFilter of Predicate
   * @param pointerMap         of Map
   */
  @ConstructorProperties({"coercibleType", "fieldDeclaration", "defaultValueFilter", "pointerMap"})
  public JSONCreateFunction( JSONCoercibleType coercibleType, Fields fieldDeclaration, Predicate<?> defaultValueFilter, Map<Fields, String> pointerMap )
    {
    super( coercibleType, fieldDeclaration, defaultValueFilter, pointerMap );
    }
  }
