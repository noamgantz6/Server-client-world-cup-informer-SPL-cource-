package bgu.spl.net.impl.echo;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;

public class EchoStompProtocol<T> implements StompMessagingProtocol<T> {

    private int connectionId;
    private Connections<T> connections;
    private boolean shouldTerminate = false;

    @Override
    public void start(int connectionId, Connections<T> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
    }

    @Override
    public void process(T message) {
        // echo back the same message
        connections.send(connectionId, message);
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }
}
