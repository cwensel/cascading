/*
 * Copyright (c) 2007-2022 The Cascading Authors. All Rights Reserved.
 *
 * Project and contact information: https://cascading.wensel.net/
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

package cascading.tap.type;

import java.io.IOException;

import cascading.flow.FlowProcess;

/** Interface FileType marks specific platform {@link cascading.tap.Tap} classes as representing a file like interface. */
public interface FileType<Config>
  {
  String CASCADING_SOURCE_PATH = "cascading.source.path";

  /**
   * Method isDirectory returns true if the underlying resource represents a directory or folder instead
   * of an individual file.
   *
   * @param flowProcess
   * @return boolean
   * @throws java.io.IOException
   */
  boolean isDirectory( FlowProcess<? extends Config> flowProcess ) throws IOException;

  /**
   * Method isDirectory returns true if the underlying resource represents a directory or folder instead
   * of an individual file.
   *
   * @param conf of Conf
   * @return boolean
   * @throws java.io.IOException
   */
  boolean isDirectory( Config conf ) throws IOException;

  /**
   * Method getChildIdentifiers returns an array of child identifiers if this resource is a directory.
   * <p>
   * This method will skip Hadoop log directories ({@code _log}).
   *
   * @param flowProcess of type FlowProcess
   * @return String[]
   * @throws java.io.IOException
   */
  String[] getChildIdentifiers( FlowProcess<? extends Config> flowProcess ) throws IOException;

  /**
   * Method getChildIdentifiers returns an array of child identifiers if this resource is a directory.
   * <p>
   * This method will skip Hadoop log directories ({@code _log}).
   *
   * @param conf of type Conf
   * @return String[]
   * @throws IOException
   */
  String[] getChildIdentifiers( Config conf ) throws IOException;

  /**
   * Method getChildIdentifiers returns an array of child identifiers if this resource is a directory.
   * <p>
   * This method will skip Hadoop log directories ({@code _log}).
   * <p>
   * Note that all files encountered up to depth will be returned, and no directories unless
   * the directory is the final path at max depth.
   *
   * @param flowProcess
   * @param depth
   * @param fullyQualified
   * @return String{}
   * @throws IOException
   */
  String[] getChildIdentifiers( FlowProcess<? extends Config> flowProcess, int depth, boolean fullyQualified ) throws IOException;

  /**
   * Method getChildIdentifiers returns an array of child identifiers if this resource is a directory.
   * <p>
   * This method will skip Hadoop log directories ({@code _log}).
   * <p>
   * Note that all files encountered up to depth will be returned, and no directories unless
   * the directory is the final path at max depth.
   *
   * @param conf           of type Conf
   * @param depth          the max depth to return.
   * @param fullyQualified true if the returned paths should be fully qualified
   * @return String[]
   * @throws IOException
   */
  String[] getChildIdentifiers( Config conf, int depth, boolean fullyQualified ) throws IOException;

  /**
   * Method getSize returns the size of the file referenced by this tap.
   *
   * @param flowProcess
   * @return The size of the file reference by this tap.
   * @throws java.io.IOException
   */
  long getSize( FlowProcess<? extends Config> flowProcess ) throws IOException;

  /**
   * Method getSize returns the size of the file referenced by this tap.
   *
   * @param conf of type Config
   * @return The size of the file reference by this tap.
   * @throws java.io.IOException
   */
  long getSize( Config conf ) throws IOException;
  }
