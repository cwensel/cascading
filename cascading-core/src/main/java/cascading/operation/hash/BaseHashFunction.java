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

package cascading.operation.hash;

import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;
import java.util.WeakHashMap;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.SerFunction;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

/**
 * Class BaseHashFunction is the base class for Message Digest based hashing operations.
 * <p>
 * All arguments to this {@link Function} will be concatenated, hashed by the given {@code algorithm},
 * then encoded by the current encoding scheme.
 * <p>
 * If the value is null, an empty string is substituted.
 */
public abstract class BaseHashFunction extends BaseOperation<BaseHashFunction.Context>
  implements Function<BaseHashFunction.Context>
  {
  public static final String DEFAULT_ALGORITHM = "SHA-1";
  public static final String DEFAULT_CHARSET = "UTF-8";

  protected class Context
    {
    Tuple tuple = Tuple.size( 1 );
    WeakHashMap<String, String> cache = new WeakHashMap<>();
    MessageDigest digest = getDigest();
    }

  protected final String algorithm;
  protected final int maxLength;
  protected final String charsetName;
  protected final SerFunction<String, String> preDigest;
  protected final SerFunction<StringBuilder, StringBuilder> postEncoding;

  /**
   * Constructor BaseHashFunction creates a new BaseHashFunction instance.
   *
   * @param fieldDeclaration of Fields
   */
  public BaseHashFunction( Fields fieldDeclaration )
    {
    this( fieldDeclaration, DEFAULT_ALGORITHM );
    }

  /**
   * Constructor BaseHashFunction creates a new BaseHashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param preDigest        of SerFunction<String, String>
   * @param postEncoding     of SerFunction<StringBuilder, StringBuilder>
   */
  public BaseHashFunction( Fields fieldDeclaration, SerFunction<String, String> preDigest, SerFunction<StringBuilder, StringBuilder> postEncoding )
    {
    this( fieldDeclaration, DEFAULT_ALGORITHM, Integer.MAX_VALUE, DEFAULT_CHARSET, preDigest, postEncoding );
    }

  /**
   * Constructor BaseHashFunction creates a new BaseHashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param algorithm        of String
   */
  public BaseHashFunction( Fields fieldDeclaration, String algorithm )
    {
    this( fieldDeclaration, algorithm, Integer.MAX_VALUE );
    }

  /**
   * Constructor BaseHashFunction creates a new BaseHashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param algorithm        of String
   * @param preDigest        of SerFunction<String, String>
   * @param postEncoding     of SerFunction<StringBuilder, StringBuilder>
   */
  public BaseHashFunction( Fields fieldDeclaration, String algorithm, SerFunction<String, String> preDigest, SerFunction<StringBuilder, StringBuilder> postEncoding )
    {
    this( fieldDeclaration, algorithm, Integer.MAX_VALUE, DEFAULT_CHARSET, preDigest, postEncoding );
    }

  /**
   * Constructor BaseHashFunction creates a new BaseHashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param algorithm        of String
   * @param maxLength        of int
   */
  public BaseHashFunction( Fields fieldDeclaration, String algorithm, int maxLength )
    {
    this( fieldDeclaration, algorithm, maxLength, DEFAULT_CHARSET );
    }

  /**
   * Constructor BaseHashFunction creates a new BaseHashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param algorithm        of String
   * @param maxLength        of int
   * @param preDigest        of SerFunction<String, String>
   * @param postEncoding     of SerFunction<StringBuilder, StringBuilder>
   */
  public BaseHashFunction( Fields fieldDeclaration, String algorithm, int maxLength, SerFunction<String, String> preDigest, SerFunction<StringBuilder, StringBuilder> postEncoding )
    {
    this( fieldDeclaration, algorithm, maxLength, DEFAULT_CHARSET, preDigest, postEncoding );
    }

  /**
   * Constructor BaseHashFunction creates a new BaseHashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param algorithm        of String
   * @param maxLength        of int
   * @param charsetName      of String
   */
  public BaseHashFunction( Fields fieldDeclaration, String algorithm, int maxLength, String charsetName )
    {
    this( fieldDeclaration, algorithm, maxLength, charsetName, null, null );
    }

  /**
   * Constructor BaseHashFunction creates a new BaseHashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param algorithm        of String
   * @param maxLength        of int
   * @param charsetName      of String
   * @param preDigest        of SerFunction<String, String>
   * @param postEncoding     of SerFunction<StringBuilder, StringBuilder>
   */
  public BaseHashFunction( Fields fieldDeclaration, String algorithm, int maxLength, String charsetName,
                           SerFunction<String, String> preDigest,
                           SerFunction<StringBuilder, StringBuilder> postEncoding )
    {
    super( fieldDeclaration );
    this.algorithm = algorithm;
    this.charsetName = charsetName;
    this.maxLength = maxLength;
    this.preDigest = preDigest == null ? SerFunction.identity() : preDigest;
    this.postEncoding = postEncoding == null ? SerFunction.identity() : postEncoding;

    if( fieldDeclaration.size() != 1 )
      throw new IllegalArgumentException( "fieldDeclaration may only declare one field, was " + fieldDeclaration.print() );

    verify();
    }

  /**
   * Method verify ...
   */
  protected void verify()
    {
    getDigest();
    getCharset();
    }

  /**
   * Method getAlgorithm returns the algorithm of this BaseHashFunction object.
   *
   * @return the algorithm (type String) of this BaseHashFunction object.
   */
  public String getAlgorithm()
    {
    return algorithm;
    }

  /**
   * Method prepare ...
   *
   * @param flowProcess   of FlowProcess
   * @param operationCall of OperationCall<Context>
   */
  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Context> operationCall )
    {
    operationCall.setContext( new Context() );
    }

  /**
   * Method operate ...
   *
   * @param flowProcess  of FlowProcess
   * @param functionCall of FunctionCall<Context>
   */
  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Context> functionCall )
    {
    Context context = functionCall.getContext();
    Tuple result = context.tuple;

    String string = getValue( functionCall );

    if( string == null )
      string = "";

    String encoded =
      context.cache.computeIfAbsent( string, value ->
      {
      value = preDigest.apply( value );

      byte[] bytes = value.getBytes( getCharset() );
      byte[] digest = context.digest.digest( bytes ); // guaranteed single threading

      StringBuilder buffer = new StringBuilder();

      performEncoding( buffer, digest );

      buffer = postEncoding.apply( buffer );

      if( buffer.length() > maxLength )
        return buffer.substring( 0, maxLength );

      return buffer.toString();
      } );

    result.set( 0, encoded );

    functionCall.getOutputCollector().add( result );
    }

  /**
   * Method performEncoding ...
   *
   * @param buffer of StringBuilder
   * @param digest of byte[]
   */
  protected abstract void performEncoding( StringBuilder buffer, byte[] digest );

  /**
   * Method getValue ...
   *
   * @param functionCall of FunctionCall<Context>
   * @return String
   */
  protected String getValue( FunctionCall<Context> functionCall )
    {
    // if one argument
    if( functionCall.getArguments().size() == 1 )
      return functionCall.getArguments().getString( 0 );

    // if many arguments
    Iterator<String> values = functionCall.getArguments().asIterableOf( String.class ).iterator();
    StringBuilder result = new StringBuilder();

    while( values.hasNext() )
      {
      String next = values.next();

      if( next != null )
        result.append( next );
      }

    return result.toString();
    }

  /**
   * Method getDigest returns the digest of this BaseHashFunction object.
   *
   * @return the digest (type MessageDigest) of this BaseHashFunction object.
   */
  protected MessageDigest getDigest()
    {
    try
      {
      return MessageDigest.getInstance( getAlgorithm() );
      }
    catch( NoSuchAlgorithmException exception )
      {
      throw new CascadingException( "unknown digest algorithm: " + getAlgorithm(), exception );
      }
    }

  /**
   * Method getCharset returns the charset of this BaseHashFunction object.
   *
   * @return the charset (type Charset) of this BaseHashFunction object.
   */
  protected Charset getCharset()
    {
    return Charset.forName( charsetName );
    }
  }
