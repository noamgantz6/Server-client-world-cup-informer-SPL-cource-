package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.impl.data.Database;
import bgu.spl.net.srv.Server;
import java.util.function.Supplier;

import javax.xml.crypto.Data;

public class StompServer {
    public static void main(String[] args) {
        final int REACTOR_THREADS = 4; // Number of threads for the reactor mode
        if (args == null || args.length != 2) {
            System.out.println("Usage: StompServer <port> <reactor|tpc>");
            return;
        }
        final int port;
        try {
            port = Integer.parseInt(args[0]); // the port number
        } catch (NumberFormatException e) { // invalid port
            System.out.println("Invalid port: " + args[0]);
            return;
        }
        final String mode = args[1]; // server mode
        Supplier<StompMessagingProtocol<String>> protocolFactory = StompMessagingProtocolImpl::new;
        Supplier<MessageEncoderDecoder<String>> encdecFactory = StompMessageEncoderDecoder::new;
        Server<String> server;
        if (mode.equalsIgnoreCase("reactor")) {
            server = Server.reactor(REACTOR_THREADS, port, protocolFactory, encdecFactory);
        } else {
            server = Server.threadPerClient(port, protocolFactory, encdecFactory);
        }

        server.serve();

    }
}
