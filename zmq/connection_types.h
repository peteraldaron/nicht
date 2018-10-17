#pragma once

#include <cstdint>

namespace mq {
    enum class event : uint8_t {
        REGISTER = 0,
        SUBSCRIBE = 1,
        PUBLISH = 2,
        DISCONNECT = 3
    };
}