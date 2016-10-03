/*
 * Copyright (c) 2016 Chris K Wensel <chris@wensel.net>. All Rights Reserved.
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

package cascading.flow.stream.duct;

/**
 *
 */
public class OrdinalDuct<Incoming, Outgoing> extends Duct<Incoming, Outgoing>
  {
  int ordinal = 0;

  public OrdinalDuct( Duct next, int ordinal )
    {
    super( next );
    this.ordinal = ordinal;
    }

  public void receive( Duct previous, int ordinal, Incoming incoming )
    {
    // override ordinal value
    next.receive( previous, this.ordinal, (Outgoing) incoming );
    }

  @Override
  public String toString()
    {
    final StringBuilder sb = new StringBuilder();
    sb.append( getClass().getSimpleName() );
    sb.append( "{ordinal=" ).append( ordinal );
    sb.append( "{next=" ).append( next );
    sb.append( '}' );
    return sb.toString();
    }

  }
