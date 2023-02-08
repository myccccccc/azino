#include "bthread/bthread.h"

#ifndef AZINO_BACKGROUND_TASK_H
#define AZINO_BACKGROUND_TASK_H

namespace azino {
class BackgroundTask {
   public:
    BackgroundTask() = default;
    ~BackgroundTask() = default;
    int Start() {
        {
            std::lock_guard<bthread::Mutex> lck(_mutex);
            if (fn == nullptr) {
                return ENOENT;
            }
            if (!_stopped) {
                return -1;
            } else {
                _stopped = false;
            }
        }
        return bthread_start_background(&_bid, NULL, fn, this);
    }

    int Stop() {
        {
            // reset _stopped, if the bthread wake up and found _stopped, it
            // will exit.
            std::lock_guard<bthread::Mutex> lck(_mutex);
            if (_stopped) {
                return -1;
            } else {
                _stopped = true;
            }
        }

        bthread_stop(_bid);
        return bthread_join(_bid, NULL);
    }

   protected:
    void* (*fn)(void*) = nullptr;
    bthread::Mutex _mutex;
    bool _stopped = true;

   private:
    bthread_t _bid = -1;
};
}  // namespace azino
#endif  // AZINO_BACKGROUND_TASK_H
