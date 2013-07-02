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

package cascading.tuple.coerce;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import cascading.cascade.CascadeException;
import cascading.tuple.Fields;
import cascading.tuple.type.CoercibleType;
import cascading.util.Util;

/**
 * Coercions class is a helper class for managing primitive value coercions.
 * <p/>
 * The {@link Coerce} constants are the default coercions for the specified type.
 * <p/>
 * To override the behavior, you must create a new {@link CoercibleType} and assign it to the {@link Fields}
 * field position/name the custom behavior should apply.
 * <p/>
 * Coercions are always used if {@link cascading.tuple.Tuple} elements are accessed via a {@link cascading.tuple.TupleEntry}
 * wrapper instance.
 *
 * @see CoercibleType
 */
public final class Coercions
  {
  public static abstract class Coerce<T> implements CoercibleType<T>
    {
    protected Coerce( Map<Type, Coerce> map )
      {
      if( map.containsKey( getCanonicalType() ) )
        throw new IllegalStateException( "type already exists in map: " + getCanonicalType() );

      map.put( getCanonicalType(), this );
      }

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

  private static final Map<String, Type> typesPrivate = new HashMap<String, Type>();
  public static final Map<String, Type> types = Collections.unmodifiableMap( typesPrivate );

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
  public static final Coerce<BigDecimal> BIG_DECIMAL = new BigDecimalCoerce( coercionsPrivate );

  static
    {
    for( Type type : coercionsPrivate.keySet() )
      typesPrivate.put( Util.getTypeName( type ), type );
    }

  private static final Map<Class, Class> primitivesPrivate = new IdentityHashMap<Class, Class>();
  public static final Map<Class, Class> primitives = Collections.unmodifiableMap( primitivesPrivate );

  static
    {
    primitivesPrivate.put( Boolean.TYPE, Boolean.class );
    primitivesPrivate.put( Byte.TYPE, Byte.class );
    primitivesPrivate.put( Short.TYPE, Short.class );
    primitivesPrivate.put( Integer.TYPE, Integer.class );
    primitivesPrivate.put( Long.TYPE, Long.class );
    primitivesPrivate.put( Float.TYPE, Float.class );
    primitivesPrivate.put( Double.TYPE, Double.class );
    }

  /**
   * Returns the primitive wrapper fo the given type, if the given type represents a primitive, otherwise
   * the type is returned.
   *
   * @param type of type Class
   * @return a Class
   */
  public static Class asNonPrimitive( Class type )
    {
    if( type.isPrimitive() )
      return primitives.get( type );

    return type;
    }

  /**
   * Method coercibleTypeFor returns the {@link CoercibleType} for the given {@link Type} instance.
   * <p/>
   * If type is null, the {@link #OBJECT} CoercibleType is returned.
   * <p/>
   * If type is an instance of CoercibleType, the given type is returned.
   * <p/>
   * If no mapping is found, an {@link IllegalStateException} will be thrown.
   *
   * @param type the type to look up
   * @return a CoercibleType for the given type
   */
  public static CoercibleType coercibleTypeFor( Type type )
    {
    if( type == null )
      return OBJECT;

    if( CoercibleType.class.isInstance( type ) )
      return (CoercibleType) type;

    Coerce coerce = coercionsPrivate.get( type );

    if( coerce == null )
      return OBJECT;

    return coerce;
    }

  /**
   * Method coerce will coerce the given value to the given type using any {@link CoercibleType} mapping available.
   * <p/>
   * If no mapping is found, the {@link #OBJECT} CoercibleType will be use.
   *
   * @param value the value to coerce, may be null.
   * @param type  the type to coerce to via any mapped CoercibleType
   * @param <T>   the type expected
   * @return the coerced value
   */
  public static final <T> T coerce( Object value, Type type )
    {
    Coerce<T> coerce = coercionsPrivate.get( type );

    if( coerce == null )
      return (T) OBJECT.coerce( value );

    return coerce.coerce( value );
    }

  /**
   * Method coercibleArray will return an array of {@link CoercibleType} instances based on the
   * given field type information. Each element of {@link cascading.tuple.Fields#getTypes()}
   * will be used to lookup the corresponding CoercibleType.
   *
   * @param fields an instance of Fields with optional type information.
   * @return array of CoercibleType
   */
  public static CoercibleType[] coercibleArray( Fields fields )
    {
    return coercibleArray( fields.size(), fields.getTypes() );
    }

  /**
   * Method coercibleArray will return an array of {@link CoercibleType} instances based on the
   * given type array. Each element of the type array
   * will be used to lookup the corresponding CoercibleType.
   *
   * @param size  the size of the expected array, must equal {@code types.length} if {@code types != null}
   * @param types an array of types to lookup
   * @return array of CoercibleType
   */
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

  /**
   * Method asClass is a convenience method for casting the given type to a {@link Class} if an instance of Class
   * or to {@link Object} if not.
   *
   * @param type of type Type
   * @return of type Class
   */
  public static Class asClass( Type type )
    {
    if( Class.class.isInstance( type ) )
      return (Class) type;

    return Object.class;
    }

  /**
   * Method asType is a convenience method for looking up a type name (like {@code "int"} or {@code "java.lang.String"}
   * to its corresponding {@link Class} or instance of CoercibleType.
   * <p/>
   * If the name is not in the {@link #types} map, the classname will be loaded from the current {@link ClassLoader}.
   *
   * @param typeName a string class or type nam.
   * @return an instance of the requested type class.
   */
  public static Type asType( String typeName )
    {
    Type type = typesPrivate.get( typeName );

    if( type != null )
      return type;

    Class typeClass = getType( typeName );

    if( CoercibleType.class.isAssignableFrom( typeClass ) )
      return getInstance( typeClass );

    return typeClass;
    }

  private static CoercibleType getInstance( Class<CoercibleType> typeClass )
    {
    try
      {
      return typeClass.newInstance();
      }
    catch( Exception exception )
      {
      throw new CascadeException( "unable to instantiate class: " + Util.getTypeName( typeClass ) );
      }
    }

  private static Class<?> getType( String typeName )
    {
    try
      {
      return Coercions.class.getClassLoader().loadClass( typeName );
      }
    catch( ClassNotFoundException exception )
      {
      throw new CascadeException( "unable to load class: " + typeName );
      }
    }
  }
