#ifndef _ARIATHREAD_H_
#define _ARIATHREAD_H_

#include "global.h"

class AriaSequencerThread : public Thread {
public:
    RC run();
    void setup();
};

#endif
