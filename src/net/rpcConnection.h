#pragma once

#include "rpcServer.h"
#include "net/globals.h"

#include <boost/asio/deadline_timer.hpp>

class RPCConnection final
{
    public:
        RPCConnection(tcp::socket* socket);

    private:
        void handle_read(const boost::system::error_code& ec, size_t bytes);
        void handle_write(const boost::system::error_code&, size_t);
        void handle_timeout(const boost::system::error_code& ec);
        void finish();

        tcp::socket* m_socket;
        boost::asio::streambuf m_buffer;
        boost::asio::deadline_timer m_timeout;
};
