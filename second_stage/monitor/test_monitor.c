#include <stdio.h>

#include "monitor.h"
#include "event_manager.h"

int main() {

    event_manager_p manager = event_manager_init();

    monitor_p monitor = monitor_init(manager);

    while(1);

    return 0;
}