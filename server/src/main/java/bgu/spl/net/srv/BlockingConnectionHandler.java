package bgu.spl.net.srv;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.MessagingProtocol;
import bgu.spl.net.api.StompMessagingProtocol;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.net.Socket;

public class BlockingConnectionHandler<T> implements Runnable, ConnectionHandler<T> {

    private final MessagingProtocol<T> protocol;
    private final StompMessagingProtocol<T> protocol1;
    private final MessageEncoderDecoder<T> encdec;
    private final Socket sock;
    private BufferedInputStream in;
    private BufferedOutputStream out;
    private volatile boolean connected = true;

    public BlockingConnectionHandler(Socket sock, MessageEncoderDecoder<T> reader, MessagingProtocol<T> protocol) {
        this.sock = sock;
        this.encdec = reader;
        this.protocol = protocol;
        this.protocol1 = null;
    }

    public BlockingConnectionHandler(Socket sock, MessageEncoderDecoder<T> reader, StompMessagingProtocol<T> protocol) {
        this.protocol = null;
        this.sock = sock;
        this.encdec = reader;
        this.protocol1 = protocol;
    }

    @Override
    public void run() {
        try (Socket sock = this.sock) { // just for automatic closing

            in = new BufferedInputStream(sock.getInputStream());
            out = new BufferedOutputStream(sock.getOutputStream());
            if (protocol1 == null) {
                return;
            }
            int read;
            while (!protocol1.shouldTerminate() && connected && (read = in.read()) != -1) {
                T nextMessage = encdec.decodeNextByte((byte) read);
                if (nextMessage != null) {
                    protocol1.process(nextMessage);
                }
            }

        } catch (IOException ex) {
            // connection reset or other IO error - mark as disconnected
            connected = false;
        } finally {
            try {
                close();
            } catch (IOException ignored) {
            }
        }

    }

    @Override
    public void close() throws IOException {
        connected = false;
        sock.close();
    }

    @Override
    public synchronized void send(T msg) { // synchronized to prevent concurrent writes
        try {
            out.write(encdec.encode(msg));
            out.flush();
        } catch (IOException e) {
            connected = false; // mark as disconnected on failure
        }
    }

}
