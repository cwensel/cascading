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

package cascading.tap.hadoop;

import java.util.Map;
import java.util.Properties;

import cascading.property.Props;

/**
 * Class HfsProps is a fluent helper for setting various Hadoop FS level properties that some
 * {@link cascading.flow.Flow} may or may not be required to have set. These properties are typically passed to a Flow
 * via a {@link cascading.flow.FlowConnector}.
 */
public class HfsProps extends Props
  {
  /** Field TEMPORARY_DIRECTORY */
  public static final String TEMPORARY_DIRECTORY = "cascading.tmp.dir";
  /** Fields LOCAL_MODE_SCHEME * */
  public static final String LOCAL_MODE_SCHEME = "cascading.hadoop.localmode.scheme";
  /** Field COMBINE_FILES_FOR_INPUT */
  public static final String COMBINE_INPUT_FILES = "cascading.hadoop.hfs.combine.files";
  /** Field COMBINE_INPUT_FILES_SIZE_MAX */
  public static final String COMBINE_INPUT_FILES_SIZE_MAX = "cascading.hadoop.hfs.combine.max.size";

  protected String temporaryDirectory;
  protected String localModeScheme;
  protected Boolean useCombinedInput;
  protected Long combinedInputMaxSize;

  /**
   * Method setTemporaryDirectory sets the temporary directory on the given properties object.
   *
   * @param properties         of type Map<Object,Object>
   * @param temporaryDirectory of type String
   */
  public static void setTemporaryDirectory( Map<Object, Object> properties, String temporaryDirectory )
    {
    properties.put( TEMPORARY_DIRECTORY, temporaryDirectory );
    }

  /**
   * Method setLocalModeScheme provides a means to change the scheme value used to detect when a
   * MapReduce job should be run in Hadoop local mode. By default the value is {@code "file"}, set to
   * {@code "none"} to disable entirely.
   *
   * @param properties of type Map<Object,Object>
   * @param scheme     a String
   */
  public static void setLocalModeScheme( Map<Object, Object> properties, String scheme )
    {
    properties.put( LOCAL_MODE_SCHEME, scheme );
    }

  /**
   * Method setUseCombinedInput provides a means to indicate whether to leverage
   * {@link org.apache.hadoop.mapred.lib.CombineFileInputFormat} for the input format. By default it is false.
   * <p/>
   * Use {@link #setCombinedInputMaxSize(long)} to set the max split/combined input size. Other specific
   * properties must be specified directly if needed. Specifically "mapred.min.split.size.per.node" and
   * "mapred.min.split.size.per.rack", which are 0 by default.
   *
   * @param properties of type Map<Object,Object>
   * @param combine    a boolean
   */
  public static void setUseCombinedInput( Map<Object, Object> properties, Boolean combine )
    {
    if( combine != null )
      properties.put( COMBINE_INPUT_FILES, Boolean.toString( combine ) );
    }

  /**
   * Method setCombinedInputMaxSize sets the maximum input split size to be used.
   * <p/>
   * This property is an alias for the Hadoop property "mapred.max.split.size".
   *
   * @param properties of type Map<Object,Object>
   * @param size       of type long
   */
  public static void setCombinedInputMaxSize( Map<Object, Object> properties, Long size )
    {
    if( size != null )
      properties.put( COMBINE_INPUT_FILES_SIZE_MAX, Long.toString( size ) );
    }

  public HfsProps()
    {
    }

  public String getTemporaryDirectory()
    {
    return temporaryDirectory;
    }

  /**
   * Method setTemporaryDirectory sets the temporary directory for use on the underlying filesystem.
   *
   * @param temporaryDirectory of type String
   * @return returns this instance
   */
  public HfsProps setTemporaryDirectory( String temporaryDirectory )
    {
    this.temporaryDirectory = temporaryDirectory;

    return this;
    }

  public String getLocalModeScheme()
    {
    return localModeScheme;
    }

  /**
   * Method setLocalModeScheme provides a means to change the scheme value used to detect when a
   * MapReduce job should be run in Hadoop local mode. By default the value is {@code "file"}, set to
   * {@code "none"} to disable entirely.
   *
   * @param localModeScheme of type String
   * @return returns this instance
   */
  public HfsProps setLocalModeScheme( String localModeScheme )
    {
    this.localModeScheme = localModeScheme;

    return this;
    }

  public boolean isUseCombinedInput()
    {
    return useCombinedInput;
    }

  /**
   * Method setUseCombinedInput provides a means to indicate whether to leverage
   * {@link org.apache.hadoop.mapred.lib.CombineFileInputFormat} for the input format. By default it is false.
   *
   * @param useCombinedInput boolean
   * @return returns this instance
   */
  public HfsProps setUseCombinedInput( boolean useCombinedInput )
    {
    this.useCombinedInput = useCombinedInput;

    return this;
    }

  public Long getCombinedInputMaxSize()
    {
    return combinedInputMaxSize;
    }

  /**
   * Method setCombinedInputMaxSize sets the maximum input split size to be used.
   * <p/>
   * This value is not honored unless {@link #setUseCombinedInput(boolean)} is {@code true}.
   *
   * @param combinedInputMaxSize of type long
   * @return returns this instance
   */
  public HfsProps setCombinedInputMaxSize( long combinedInputMaxSize )
    {
    this.combinedInputMaxSize = combinedInputMaxSize;

    return this;
    }

  @Override
  protected void addPropertiesTo( Properties properties )
    {
    setTemporaryDirectory( properties, temporaryDirectory );
    setLocalModeScheme( properties, localModeScheme );
    setUseCombinedInput( properties, useCombinedInput );
    setCombinedInputMaxSize( properties, combinedInputMaxSize );
    }
  }
