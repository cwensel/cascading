/*
 * Copyright (c) 2008, Your Corporation. All Rights Reserved.
 */

package cascading.pipe;

/**
 * *** Experimental - May be removed in a future iteration ***
 * <p/>
 * Class EndPipe marks the end of a branch in a pipe assembly. Strictly this is not necessary, but it may allow
 * for tagging meta-data to a Tap instance that comes immediately after it. It will also allow for branching
 * after the Group to be handled in the reducer so that a reducer can write out to multiple output files.
 * <p/>
 * Currently this tagging is used to mark a tap to by pass the default OutputCollector and to write 'directly' via
 * a Tap TapCollector.
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
