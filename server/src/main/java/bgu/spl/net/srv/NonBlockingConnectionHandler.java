package bgu.spl.net.srv;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.StompMessagingProtocol;

public class NonBlockingConnectionHandler<T> implements ConnectionHandler<T> {

    private static final int BUFFER_ALLOCATION_SIZE = 1 << 13; // 8k
    private static final ConcurrentLinkedQueue<ByteBuffer> BUFFER_POOL = new ConcurrentLinkedQueue<>();

    private final StompMessagingProtocol<T> protocol;
    private final MessageEncoderDecoder<T> encdec;
    private final ConcurrentLinkedQueue<ByteBuffer> writeQueue = new ConcurrentLinkedQueue<>();
    private final SocketChannel chan;
    private final Reactor<T> reactor;

    private volatile boolean shouldClose = false;

    public NonBlockingConnectionHandler(
            MessageEncoderDecoder<T> reader,
            StompMessagingProtocol<T> protocol,
            SocketChannel chan,
            Reactor<T> reactor) {
        this.chan = chan;
        this.encdec = reader;
        this.protocol = protocol;
        this.reactor = reactor;
    }

    public Runnable continueRead() {
        if (shouldClose) // the connection is closed
            return null;
        ByteBuffer buf = leaseBuffer();
        boolean success;
        try {
            success = chan.read(buf) != -1;
        } catch (IOException ex) {
            success = false;
        }

        if (success) {
            buf.flip();
            return () -> {
                try {
                    while (buf.hasRemaining()) {
                        T nextMessage = encdec.decodeNextByte(buf.get());
                        if (nextMessage != null) {
                            protocol.process(nextMessage); // call the protocol process
                        }
                    }
                } finally {
                    releaseBuffer(buf);
                }
            };
        } else {
            releaseBuffer(buf);
            close();
            return null;
        }
    }

    public void continueWrite() { // write until there's nothing left to write
        while (!writeQueue.isEmpty()) {
            ByteBuffer top = writeQueue.peek();
            try {
                chan.write(top);
                if (top.hasRemaining()) 
                    return;
                writeQueue.remove();
            } catch (IOException ex) {
                close();
                return;
            }
        }

        if (writeQueue.isEmpty()) { 
            if (protocol.shouldTerminate()) // close if the protocol says so
                close();
            else
                reactor.updateInterestedOps(chan, SelectionKey.OP_READ); 
        }
    }

    @Override
    public void send(T msg) {
        if (shouldClose) 
            return;
        byte[] bytes = encdec.encode(msg); // encode the message to bytes
        writeQueue.add(ByteBuffer.wrap(bytes)); // add to the write queue
        reactor.updateInterestedOps(chan, SelectionKey.OP_READ | SelectionKey.OP_WRITE); // set interestOps to READ|WRITE
    }

    @Override
    public void close() {
        try {
            SelectionKey key = chan.keyFor(reactor.getSelector()); // get the selection key 
            if (key != null)
                key.cancel(); // cancel the key

            if (!isClosed()) {
                chan.close(); // close the channel
            }
        } catch (IOException ex) {
        }
    }

    private static ByteBuffer leaseBuffer() {
        ByteBuffer buff = BUFFER_POOL.poll();
        if (buff == null)
            return ByteBuffer.allocateDirect(BUFFER_ALLOCATION_SIZE);
        buff.clear();
        return buff;
    }

    private static void releaseBuffer(ByteBuffer buff) {
        BUFFER_POOL.add(buff);
    }

    public boolean isClosed() {
        return shouldClose || !chan.isOpen();
    }
}