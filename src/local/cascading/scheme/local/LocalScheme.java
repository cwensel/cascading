/*
 * Copyright (c) 2007-2011 Concurrent, Inc. All Rights Reserved.
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

package cascading.scheme.local;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.util.Properties;

import cascading.flow.local.LocalFlowProcess;
import cascading.scheme.Scheme;
import cascading.tuple.Fields;

/**
 *
 */
public abstract class LocalScheme<Input, Output, Context> extends Scheme<LocalFlowProcess, Properties, Input, Output, Context>
  {
  protected LocalScheme( Fields sourceFields )
    {
    super( sourceFields );
    }

  protected LocalScheme( Fields sourceFields, Fields sinkFields )
    {
    super( sourceFields, sinkFields );
    }

  public abstract Input createInput( FileInputStream inputStream );

  public abstract Output createOutput( FileOutputStream outputStream );
  }
