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

import java.beans.ConstructorProperties;
import java.math.BigInteger;

import cascading.operation.SerFunction;
import cascading.tuple.Fields;

/**
 * Class Base10HashFunction is a sub-class of {@link BaseHashFunction} that Base 10 encodes the digest hash value.
 * <p>
 * This class calls {@code new BigInteger( 1, digest ).toString() }
 */
public class Base10HashFunction extends BaseHashFunction
  {
  /**
   * Constructor Base10HashFunction creates a new Base10HashFunction instance.
   *
   * @param fieldDeclaration of Fields
   */
  @ConstructorProperties("fieldDeclaration")
  public Base10HashFunction( Fields fieldDeclaration )
    {
    super( fieldDeclaration );
    }

  /**
   * Constructor Base10HashFunction creates a new Base10HashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param preDigest        of SerFunction<String, String>
   * @param postEncoding     of SerFunction<StringBuilder, StringBuilder>
   */
  @ConstructorProperties({"fieldDeclaration", "preDigest", "postEncoding"})
  public Base10HashFunction( Fields fieldDeclaration, SerFunction<String, String> preDigest, SerFunction<StringBuilder, StringBuilder> postEncoding )
    {
    super( fieldDeclaration, preDigest, postEncoding );
    }

  /**
   * Constructor Base10HashFunction creates a new Base10HashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param algorithm        of String
   */
  @ConstructorProperties({"fieldDeclaration", "algorithm"})
  public Base10HashFunction( Fields fieldDeclaration, String algorithm )
    {
    super( fieldDeclaration, algorithm );
    }

  /**
   * Constructor Base10HashFunction creates a new Base10HashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param algorithm        of String
   * @param preDigest        of SerFunction<String, String>
   * @param postEncoding     of SerFunction<StringBuilder, StringBuilder>
   */
  @ConstructorProperties({"fieldDeclaration", "algorithm", "preDigest", "postEncoding"})
  public Base10HashFunction( Fields fieldDeclaration, String algorithm, SerFunction<String, String> preDigest, SerFunction<StringBuilder, StringBuilder> postEncoding )
    {
    super( fieldDeclaration, algorithm, preDigest, postEncoding );
    }

  /**
   * Constructor Base10HashFunction creates a new Base10HashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param algorithm        of String
   * @param maxLength        of int
   */
  @ConstructorProperties({"fieldDeclaration", "algorithm", "maxLength"})
  public Base10HashFunction( Fields fieldDeclaration, String algorithm, int maxLength )
    {
    super( fieldDeclaration, algorithm, maxLength );
    }

  /**
   * Constructor Base10HashFunction creates a new Base10HashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param algorithm        of String
   * @param maxLength        of int
   * @param preDigest        of SerFunction<String, String>
   * @param postEncoding     of SerFunction<StringBuilder, StringBuilder>
   */
  @ConstructorProperties({"fieldDeclaration", "algorithm", "maxLength", "preDigest", "postEncoding"})
  public Base10HashFunction( Fields fieldDeclaration, String algorithm, int maxLength, SerFunction<String, String> preDigest, SerFunction<StringBuilder, StringBuilder> postEncoding )
    {
    super( fieldDeclaration, algorithm, maxLength, preDigest, postEncoding );
    }

  /**
   * Constructor Base10HashFunction creates a new Base10HashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param algorithm        of String
   * @param maxLength        of int
   * @param charsetName      of String
   */
  @ConstructorProperties({"fieldDeclaration", "algorithm", "maxLength", "charsetName"})
  public Base10HashFunction( Fields fieldDeclaration, String algorithm, int maxLength, String charsetName )
    {
    super( fieldDeclaration, algorithm, maxLength, charsetName );
    }

  /**
   * Constructor Base10HashFunction creates a new Base10HashFunction instance.
   *
   * @param fieldDeclaration of Fields
   * @param algorithm        of String
   * @param maxLength        of int
   * @param charsetName      of String
   * @param preDigest        of SerFunction<String, String>
   * @param postEncoding     of SerFunction<StringBuilder, StringBuilder>
   */
  @ConstructorProperties({"fieldDeclaration", "algorithm", "maxLength", "charsetName", "preDigest", "postEncoding"})
  public Base10HashFunction( Fields fieldDeclaration, String algorithm, int maxLength, String charsetName, SerFunction<String, String> preDigest, SerFunction<StringBuilder, StringBuilder> postEncoding )
    {
    super( fieldDeclaration, algorithm, maxLength, charsetName, preDigest, postEncoding );
    }

  /**
   * Method performEncoding ...
   *
   * @param buffer of StringBuilder
   * @param digest of byte[]
   */
  @Override
  protected void performEncoding( StringBuilder buffer, byte[] digest )
    {
    buffer.append( new BigInteger( 1, digest ).toString() );
    }
  }
