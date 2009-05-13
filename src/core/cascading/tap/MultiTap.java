/*
 * Copyright (c) 2007-2009 Concurrent, Inc. All Rights Reserved.
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

package cascading.tap;

import cascading.scheme.Scheme;

/**
 * This class has been deprecated in favor of {@link MultiSourceTap}.
 * <p/>
 * Class MultiTap is used to tie multiple {@link Tap} instances into a single resource. Effectively this will allow
 * multiple files to be concatenated into the requesting pipe assembly, if they all share the same {@link Scheme} instance.
 * <p/>
 * Note that order is not maintained by virtue of the underlying model. If order is necessary, use a unique sequence key
 * to span the resources, like a line number.
 * </p>
 * Note that if multiple input files have the same Scheme (like {@link cascading.scheme.TextLine}), they may not contain
 * the same semi-structure internally. For example, one file might be an Apache log file, and anoter might be a Log4J
 * log file. If each one should be parsed differently, then they must be handled by different pipe assembly branches.
 *
 * @deprecated
 */
@Deprecated
public class MultiTap extends MultiSourceTap
  {
  protected MultiTap( Scheme scheme )
    {
    super( scheme );
    }

  public MultiTap( Tap... taps )
    {
    super( taps );
    }
  }
