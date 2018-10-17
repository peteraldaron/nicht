#include <zmq.hpp>
#include <string>
#include <fmt/format.h>
#include <iostream>
#include <deque>
#include <mutex>
#include <memory>
#include "message.h"
#include "connection_types.h"
#include <condition_variable>

#define SERVER_PORT 15672

struct exchange_t {
    std::string name;
    std::mutex mut;
    std::vector<size_t> subscribers;
    std::deque<std::string> queue;
    exchange_t() = delete; //{}
    exchange_t(const std::string &name) : name(name) {}
};

typedef std::unordered_map<std::string, exchange_t*> exchange_lookup_map;

std::string frame_to_string(mq::msg_frame &frame) {
    return fmt::format("Exchange: {}\nBody: {}", frame.exchange, frame.body);
}

void main_event_loop() {
    std::mutex exchange_lookup_mutex, incoming_queue_mutex;
    std::condition_variable cv;
    auto incoming_message_queue = std::make_shared<std::deque<mq::msg_frame> >();
    auto exchanges = std::make_shared<exchange_lookup_map>();
    while (true) {
        std::unique_lock<std::mutex> lock(incoming_queue_mutex);
        cv.wait(lock, [&]{return !incoming_message_queue->empty();});
        // lock acquired
        mq::msg_frame next_msg = std::move(incoming_message_queue->back());
        incoming_message_queue->pop_back();
        lock.unlock();
        switch (next_msg.type) {
            case mq::event::REGISTER: {
                std::lock_guard<std::mutex> ex_lock(exchange_lookup_mutex);
                if (!exchanges->count(next_msg.exchange)) {
                    auto ex = new exchange_t(next_msg.exchange);
                    exchanges->operator[](next_msg.exchange) = ex;
                }
                break;
            }
            case mq::event::SUBSCRIBE: {
                std::lock_guard<std::mutex> ex_lock(exchange_lookup_mutex);
                if (!exchanges->count(next_msg.exchange)) {
                    auto ex = new exchange_t(next_msg.exchange);
                    exchanges->operator[](next_msg.exchange) = ex;
                }
                break;
            }
        }
    }
    // std::shared_lock<std::shared_mutex> lock(mutex_);
}

int main() {
    zmq::context_t ctx;
    zmq::socket_t socket(ctx, zmq::socket_type::router);
    zmq::socket_t responder(ctx, zmq::socket_type::pub);
    const std::string server_addr = fmt::format("tcp://*:{}", SERVER_PORT);
    const std::string responder_addr = fmt::format("tcp://*:{}", 15671);
    std::cout<<"Listening on port "<<server_addr<<std::endl;
    std::cout<<"Message frame size: " << sizeof(mq::msg_frame) <<std::endl;
    socket.bind(server_addr);
    responder.bind(responder_addr);

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
    //  Switch messages between sockets
    zmq::pollitem_t item{ socket,  0, ZMQ_POLLIN, 0 };
    zmq::message_t message;
    mq::msg_frame frame;
    while (true) {
        //zmq::poll(&item, 1, -1);
        socket.recv(&message); //discarded envelop msg
        socket.recv(&message);
        memcpy(&frame, message.data(), message.size());
        std::cout << frame_to_string(frame) << std::endl;
    }
#pragma clang diagnostic pop
    return 0;
}