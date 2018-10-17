#pragma once

#include <cstdint>
#include "connection_types.h"

namespace mq {
    struct msg_frame {
        mq::event type;
        char exchange[120];
        char body[1024];
    };
}
