/*
 * Copyright (c) 2016-2017 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
 * Copyright (c) 2007-2017 Xplenty, Inc. All Rights Reserved.
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

package cascading.operation.regex;

import java.beans.ConstructorProperties;
import java.util.Arrays;
import java.util.regex.Matcher;

import cascading.CascadingException;
import cascading.flow.FlowProcess;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.operation.OperationException;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.tuple.Tuples;
import cascading.util.Pair;

/**
 * Class RegexParser is used to extract a matched regex from an incoming argument value.
 * <p/>
 * RegexParser only expects one field value. If more than one argument value is passed, only the
 * first is handled, the remainder are ignored.
 * <p/>
 * Sometimes its useful to parse out a value from a key/value pair in a string, if the key exists. If the key does
 * not exist, returning an empty string instead of failing is typically expected.
 * <p/>
 * The following regex can extract a value from {@code key1=value1&key2=value2} if key1 exists, otherwise an
 * empty string is returned:<br/>
 * <pre>(?<=key1=)[^&]*|$</pre>
 * <p/>
 * Note a {@code null} valued argument passed to the parser will be converted to an empty string ({@code ""}) before
 * the regex is applied.
 * <p/>
 * Any Object value will be coerced to a String type if type information is provided. See the
 * {@link cascading.tuple.type.CoercibleType} interface to control how custom Object types are converted to String
 * values.
 * <p/>
 * Also, any type information on the declaredFields will also be honored by coercing the parsed String value to the
 * canonical declared type. This is useful when creating or using CoercibleType classes, like
 * {@link cascading.tuple.type.DateType}.
 */
public class RegexParser extends RegexOperation<Pair<Matcher, TupleEntry>> implements Function<Pair<Matcher, TupleEntry>>
  {
  /** Field groups */
  private int[] groups = null;

  /**
   * Constructor RegexParser creates a new RegexParser instance, where the argument Tuple value is matched and returned
   * in a new Tuple.
   * <p/>
   * If the given patternString declares regular expression groups, each group will be returned as a value in the
   * resulting Tuple. If no groups are declared, the match will be returned as the only value in the resulting Tuple.
   * <p/>
   * The fields returned will be {@link Fields#UNKNOWN}, so a variable number of values may be emitted based on the
   * regular expression given.
   *
   * @param patternString of type String
   */
  @ConstructorProperties({"patternString"})
  public RegexParser( String patternString )
    {
    super( 1, patternString );
    }

  /**
   * Constructor RegexParser creates a new RegexParser instance, where the argument Tuple value is matched and returned
   * as the given Field.
   * <p/>
   * If the given patternString declares regular expression groups, each group will be returned as a value in the
   * resulting Tuple. If no groups are declared, the match will be returned as the only value in the resulting Tuple.
   * <p/>
   * If the number of fields in the fieldDeclaration does not match the number of groups matched, an {@link OperationException}
   * will be thrown during runtime.
   * <p/>
   * To overcome this, either use the constructors that take an array of groups, or use the {@code (?: ...)} sequence
   * to tell the regular expression matcher to not capture the group.
   *
   * @param fieldDeclaration of type Fields
   * @param patternString    of type String
   */
  @ConstructorProperties({"fieldDeclaration", "patternString"})
  public RegexParser( Fields fieldDeclaration, String patternString )
    {
    super( 1, fieldDeclaration, patternString );
    }

  /**
   * Constructor RegexParser creates a new RegexParser instance, where the patternString is a regular expression
   * with match groups and whose groups designated by {@code groups} are stored in the appropriate number of new fields.
   * <p/>
   * The number of resulting fields will match the number of groups given ({@code groups.length}).
   *
   * @param patternString of type String
   * @param groups        of type int[]
   */
  @ConstructorProperties({"patternString", "groups"})
  public RegexParser( String patternString, int... groups )
    {
    super( 1, Fields.size( verifyReturnLength( groups ) ), patternString );

    this.groups = Arrays.copyOf( groups, groups.length );
    }

  private static int verifyReturnLength( int[] groups )
    {
    if( groups == null || groups.length == 0 )
      throw new IllegalArgumentException( "groups may not be null or 0 length" );

    return groups.length;
    }

  /**
   * Constructor RegexParser creates a new RegexParser instance, where the patternString is a regular expression
   * with match groups and whose groups designated by {@code groups} are stored in the named fieldDeclarations.
   *
   * @param fieldDeclaration of type Fields
   * @param patternString    of type String
   * @param groups           of type int[]
   */
  @ConstructorProperties({"fieldDeclaration", "patternString", "groups"})
  public RegexParser( Fields fieldDeclaration, String patternString, int... groups )
    {
    super( 1, fieldDeclaration, patternString );

    verifyReturnLength( groups );

    this.groups = Arrays.copyOf( groups, groups.length );

    if( !fieldDeclaration.isUnknown() && fieldDeclaration.size() != groups.length )
      throw new IllegalArgumentException( "fieldDeclaration must equal number of groups to be captured, fields: " + fieldDeclaration.print() );
    }

  public int[] getGroups()
    {
    if( groups == null )
      return null;

    return Arrays.copyOf( groups, groups.length );
    }

  @Override
  public void prepare( FlowProcess flowProcess, OperationCall<Pair<Matcher, TupleEntry>> operationCall )
    {
    int size;

    if( groups != null )
      size = groups.length;
    else
      size = operationCall.getDeclaredFields().size(); // if Fields.UNKNOWN size will be zero

    // TupleEntry allows us to honor the declared field type information
    TupleEntry entry = new TupleEntry( operationCall.getDeclaredFields(), Tuple.size( size ) );

    operationCall.setContext( new Pair<>( getPattern().matcher( "" ), entry ) );
    }

  @Override
  public void operate( FlowProcess flowProcess, FunctionCall<Pair<Matcher, TupleEntry>> functionCall )
    {
    String value = functionCall.getArguments().getString( 0 );

    if( value == null )
      value = "";

    Matcher matcher = functionCall.getContext().getLhs().reset( value );

    if( !matcher.find() )
      throw new OperationException( "could not match pattern: [" + getPatternString() + "] with value: [" + value + "]" );

    TupleEntry output = functionCall.getContext().getRhs();

    if( groups != null )
      onGivenGroups( functionCall, matcher, output );
    else
      onFoundGroups( functionCall, matcher, output );
    }

  private void onFoundGroups( FunctionCall<Pair<Matcher, TupleEntry>> functionCall, Matcher matcher, TupleEntry output )
    {
    int count = matcher.groupCount();

    // if UNKNOWN then the returned number fields will be of variable size
    // subsequently we must clear the tuple, and add the found values
    if( functionCall.getDeclaredFields().isUnknown() )
      addGroupsToTuple( matcher, output, count );
    else
      setGroupsOnTuple( matcher, output, count );

    // this overcomes an issue in the planner resolver where if REPLACE is declared, the declared
    // fields for the current operation are expected to match the argument fields
    functionCall.getOutputCollector().add( output.getTuple() );
    }

  private void setGroupsOnTuple( Matcher matcher, TupleEntry output, int count )
    {
    if( count == 0 )
      {
      try
        {
        output.setString( 0, matcher.group( 0 ) );
        }
      catch( Exception exception )
        {
        throw new CascadingException( "unable to set tuple value at field: " + output.getFields().get( 0 ) + ", from regex group: 0", exception );
        }
      }
    else
      {
      for( int i = 0; i < count; i++ )
        {
        try
          {
          output.setString( i, matcher.group( i + 1 ) ); // skip group 0
          }
        catch( Exception exception )
          {
          throw new CascadingException( "unable to set tuple value at field: " + output.getFields().get( i ) + ", from regex group: " + ( i + 1 ), exception );
          }
        }
      }
    }

  private void addGroupsToTuple( Matcher matcher, TupleEntry output, int count )
    {
    Tuple tuple = output.getTuple();

    Tuples.asModifiable( tuple );

    tuple.clear();

    if( count == 0 )
      {
      tuple.add( matcher.group( 0 ) );
      }
    else
      {
      for( int i = 0; i < count; i++ )
        tuple.add( matcher.group( i + 1 ) ); // skip group 0
      }
    }

  private boolean isDeclaredFieldsUnknown( Fields declaredFields )
    {
    return declaredFields.isUnknown();
    }

  private void onGivenGroups( FunctionCall<Pair<Matcher, TupleEntry>> functionCall, Matcher matcher, TupleEntry output )
    {
    for( int i = 0; i < groups.length; i++ )
      output.setString( i, matcher.group( groups[ i ] ) );

    functionCall.getOutputCollector().add( output );
    }

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( !( object instanceof RegexParser ) )
      return false;
    if( !super.equals( object ) )
      return false;

    RegexParser that = (RegexParser) object;

    if( !Arrays.equals( groups, that.groups ) )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( groups != null ? Arrays.hashCode( groups ) : 0 );
    return result;
    }
  }
