package org.apache.doris.event;

import lombok.extern.slf4j.Slf4j;
import org.apache.doris.common.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.time.Duration;

@Slf4j
public class EventSchedulerJobMgr implements Writable {
    
    
    
    @Override
    public void write(DataOutput out) throws IOException {
        Duration
    }

    public static void read(DataInput in) throws IOException {
        // nothing need to do at now
    }
}
