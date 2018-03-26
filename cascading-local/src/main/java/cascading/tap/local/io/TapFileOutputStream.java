/*
 * Copyright (c) 2016-2018 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.tap.local.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;

import cascading.tap.Tap;
import cascading.tap.TapException;

/**
 *
 */
public class TapFileOutputStream extends FileOutputStream
  {
  public TapFileOutputStream( String path, boolean append ) throws FileNotFoundException
    {
    super( prepare( null, path ), append );
    }

  public TapFileOutputStream( Tap parent, String path, boolean update ) throws FileNotFoundException
    {
    super( prepare( parent, path ), update );
    }

  private static String prepare( Tap parent, String path )
    {
    File file;

    // is the current tap path embedded in the
    if( parent == null || path.startsWith( parent.getIdentifier() ) )
      file = new File( path );
    else
      file = new File( parent.getIdentifier(), path );

    // ignore the output. will catch the failure downstream if any.
    // not ignoring the output causes race conditions with other systems writing to the same directory.
    File parentFile = file.getAbsoluteFile().getParentFile();

    if( parentFile != null && parentFile.exists() && parentFile.isFile() )
      throw new TapException( "cannot create parent directory, it already exists as a file: " + parentFile.getAbsolutePath() );

    // don't test for success, just fighting a race condition otherwise
    // will get caught downstream
    if( parentFile != null )
      parentFile.mkdirs();

    return file.getAbsolutePath();
    }
  }
