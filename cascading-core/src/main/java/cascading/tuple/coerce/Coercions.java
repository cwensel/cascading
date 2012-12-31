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

package cascading.tuple.coerce;

import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.type.CoercibleType;
import cascading.util.Util;

/**
 *
 */
public final class Coercions
  {
  public static abstract class Coerce<T> implements CoercibleType<T>
    {
    protected Coerce( Map<Type, Coerce> map )
      {
      if( map.containsKey( getType() ) )
        throw new IllegalStateException( "type already exists in map: " + getType() );

      map.put( getType(), this );
      }

    protected abstract Class<T> getType();

    @Override
    public T canonical( Object value )
      {
      return coerce( value );
      }

    @Override
    public <Coerce> Coerce coerce( Object value, Type to )
      {
      return Coercions.coerce( value, to );
      }

    public abstract T coerce( Object value );
    }

  private static final Map<Type, Coerce> coercionsPrivate = new IdentityHashMap<Type, Coerce>();

  public static final Map<Type, Coerce> coercions = Collections.unmodifiableMap( coercionsPrivate );

  public static final Coerce<Object> OBJECT = new ObjectCoerce( coercionsPrivate );
  public static final Coerce<String> STRING = new StringCoerce( coercionsPrivate );
  public static final Coerce<Character> CHARACTER = new CharacterCoerce( coercionsPrivate );
  public static final Coerce<Character> CHARACTER_OBJECT = new CharacterObjectCoerce( coercionsPrivate );
  public static final Coerce<Short> SHORT = new ShortCoerce( coercionsPrivate );
  public static final Coerce<Short> SHORT_OBJECT = new ShortObjectCoerce( coercionsPrivate );
  public static final Coerce<Integer> INTEGER = new IntegerCoerce( coercionsPrivate );
  public static final Coerce<Integer> INTEGER_OBJECT = new IntegerObjectCoerce( coercionsPrivate );
  public static final Coerce<Double> DOUBLE = new DoubleCoerce( coercionsPrivate );
  public static final Coerce<Double> DOUBLE_OBJECT = new DoubleObjectCoerce( coercionsPrivate );
  public static final Coerce<Long> LONG = new LongCoerce( coercionsPrivate );
  public static final Coerce<Long> LONG_OBJECT = new LongObjectCoerce( coercionsPrivate );
  public static final Coerce<Float> FLOAT = new FloatCoerce( coercionsPrivate );
  public static final Coerce<Float> FLOAT_OBJECT = new FloatObjectCoerce( coercionsPrivate );
  public static final Coerce<Boolean> BOOLEAN = new BooleanCoerce( coercionsPrivate );
  public static final Coerce<Boolean> BOOLEAN_OBJECT = new BooleanObjectCoerce( coercionsPrivate );

  public static CoercibleType coercibleTypeFor( Type type )
    {
    if( type == null )
      return OBJECT;

    if( CoercibleType.class.isInstance( type ) )
      return (CoercibleType) type;

    Coerce coerce = coercionsPrivate.get( type );

    if( coerce == null )
      throw new IllegalStateException( "unknown type: " + Util.getTypeName( type ) );

    return coerce;
    }

  public static final <T> T coerce( Object value, Type type )
    {
    Coerce<T> coerce = coercionsPrivate.get( type );

    if( coerce == null )
      return (T) OBJECT.coerce( value );

    return coerce.coerce( value );
    }

  public static CoercibleType[] coercibleArray( Fields fields )
    {
    return coercibleArray( fields.size(), fields.getTypes() );
    }

  public static CoercibleType[] coercibleArray( int size, Type[] types )
    {
    CoercibleType[] coercions = new CoercibleType[ size ];

    if( types == null )
      {
      Arrays.fill( coercions, OBJECT );
      return coercions;
      }

    for( int i = 0; i < types.length; i++ )
      coercions[ i ] = coercibleTypeFor( types[ i ] );

    return coercions;
    }

  public static Class asClass( Type type )
    {
    if( type instanceof Class )
      return (Class) type;

    return Object.class;
    }
  }
