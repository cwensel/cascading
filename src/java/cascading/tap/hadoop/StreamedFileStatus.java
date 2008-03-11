/*
 * Copyright (c) 2007-2008 Vinculum Technologies, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Cascading is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * Cascading is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with Cascading.  If not, see <http://www.gnu.org/licenses/>.
 */

package cascading.tap.hadoop;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;

/** Class StreamedFileStatus ... */
public class StreamedFileStatus extends FileStatus
  {
  /** Field md5Hex */
  String md5Hex;

  /**
   * Constructor StreamedFileStatus creates a new StreamedFileStatus instance.
   *
   * @param l      of type long
   * @param b      of type boolean
   * @param i      of type int
   * @param l1     of type long
   * @param l2     of type long
   * @param path   of type Path
   * @param md5Hex of type String
   */
  public StreamedFileStatus( long l, boolean b, int i, long l1, long l2, Path path, String md5Hex )
    {
    super( l, b, i, l1, l2, path );
    this.md5Hex = md5Hex;
    }

  /**
   * Method getMd5Hex returns the md5Hex of this StreamedFileStatus object.
   *
   * @return the md5Hex (type String) of this StreamedFileStatus object.
   */
  public String getMd5Hex()
    {
    return md5Hex;
    }
  }
