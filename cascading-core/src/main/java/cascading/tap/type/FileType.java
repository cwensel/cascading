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

package cascading.tap.type;

import java.io.IOException;

/** Interface FileType marks specific platform {@link cascading.tap.Tap} classes as representing a file like interface. */
public interface FileType<Config>
  {
  /**
   * Method isDirectory returns true if the underlying resource represents a directory or folder instead
   * of an individual file.
   *
   * @param conf of JobConf
   * @return boolean
   * @throws java.io.IOException
   */
  boolean isDirectory( Config conf ) throws IOException;

  /**
   * Method getChildIdentifiers returns an array of child identifiers if this resource is a directory.
   * <p/>
   * This method will skip Hadoop log directories ({@code _log}).
   *
   * @param conf of JobConf
   * @return String[]
   * @throws java.io.IOException
   */
  String[] getChildIdentifiers( Config conf ) throws IOException;

  /**
   * Method getSize returns the size of the file referenced by this tap.
   *
   * @param conf of type Config
   * @return The size of the file reference by this tap.
   * @throws java.io.IOException
   */
  long getSize( Config conf ) throws IOException;
  }
