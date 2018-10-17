#include <zmq.hpp>
#include <string>
#include <fmt/format.h>

#define SERVER_LISTEN_PORT 5672
#define SERVER_SEND_PORT 15672

int main() {
    zmq::context_t ctx;
    zmq::socket_t frontend(ctx, zmq::socket_type::router);
    zmq::socket_t backend(ctx, zmq::socket_type::dealer);
//const std::string frontend_addr = fmt::format("tcp://*:{}", SERVER_LISTEN_PORT);
//    const std::string backend_addr = fmt::format("tcp://*:{}", SERVER_SEND_PORT);
    frontend.bind("tcp://*:5672");
    backend.bind("tcp://*:15672");

    zmq::pollitem_t items [] = {
            { frontend, 0, ZMQ_POLLIN, 0 },
            { backend,  0, ZMQ_POLLIN, 0 }
    };

    //  Switch messages between sockets
    while (1) {
        zmq::message_t message;
        int more;               //  Multipart detection

        zmq::poll(&items [0], 2, -1);

        if (items [0].revents & ZMQ_POLLIN) {
            //  Process all parts of the message
            frontend.recv(&message);
            size_t more_size = sizeof (more);
            frontend.getsockopt(ZMQ_RCVMORE, &more, &more_size);
            backend.send(message, more? ZMQ_SNDMORE: 0);
        }
        if (items [1].revents & ZMQ_POLLIN) {
            //  Process all parts of the message
            backend.recv(&message);
            size_t more_size = sizeof (more);
            backend.getsockopt(ZMQ_RCVMORE, &more, &more_size);
            frontend.send(message, more? ZMQ_SNDMORE: 0);
        }
    }
    return 0;
}