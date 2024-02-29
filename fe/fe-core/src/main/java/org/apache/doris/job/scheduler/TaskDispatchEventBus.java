package org.apache.doris.job.scheduler;

import com.google.common.eventbus.EventBus;
import org.apache.doris.job.manager.TaskDisruptorGroupManager;

public class TaskDispatchEventBus {
private static final EventBus eventBus = new EventBus("TaskDispatchEventBus");

    public static void register(TaskDisruptorGroupManager taskDisruptorGroupManager) {
        eventBus.register(taskDisruptorGroupManager);
    }

    public static void post(TaskDispatchEvent event) {
        eventBus.post(event);
    }
}
