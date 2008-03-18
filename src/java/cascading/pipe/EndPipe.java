/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.pipe;

/**
 *
 */
public class EndPipe extends Pipe
  {
  public EndPipe( String name, Pipe previous )
    {
    super( name, previous );
    }

  public EndPipe( Pipe previous )
    {
    super( previous );
    }
  }
