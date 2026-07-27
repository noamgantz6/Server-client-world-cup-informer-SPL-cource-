package bgu.spl.net.srv;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;

import bgu.spl.net.api.MessageEncoderDecoder;
import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.impl.stomp.ConnectionsImpl;

public class Reactor<T> implements Server<T> {

    private final Connections<T> connections = new ConnectionsImpl<>();
    private final int port;
    private final Supplier<StompMessagingProtocol<T>> protocolFactory;
    private final Supplier<MessageEncoderDecoder<T>> readerFactory;
    private final ActorThreadPool pool;
    private Selector selector;

    private Thread selectorThread;
    private final ConcurrentLinkedQueue<Runnable> selectorTasks = new ConcurrentLinkedQueue<>();

    public Reactor(int numThreads,
            int port,
            Supplier<StompMessagingProtocol<T>> protocolFactory,
            Supplier<MessageEncoderDecoder<T>> readerFactory) {

        this.pool = new ActorThreadPool(numThreads);
        this.port = port;
        this.protocolFactory = protocolFactory;
        this.readerFactory = readerFactory;
    }

    @Override
    public void serve() {
        selectorThread = Thread.currentThread();
        try (Selector sel = Selector.open(); // open the selector
                ServerSocketChannel serverSock = ServerSocketChannel.open()) {

            this.selector = sel;

            serverSock.bind(new InetSocketAddress(port));
            serverSock.configureBlocking(false);
            serverSock.register(selector, SelectionKey.OP_ACCEPT);
            System.out.println("Server started");

            while (!Thread.currentThread().isInterrupted()) { 
                selector.select();
                runSelectionThreadTasks();

                for (SelectionKey key : selector.selectedKeys()) { // iterate over the selected keys
                    if (!key.isValid()) {
                        continue;
                    } else if (key.isAcceptable())
                        handleAccept(serverSock); // handle new connection
                    else
                        handleReadWrite(key); // handle read/write
                }
                selector.selectedKeys().clear();
            }

        } catch (ClosedSelectorException ex) {
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            pool.shutdown(); // shutdown the thread pool when done
        }
    }

    private void handleAccept(ServerSocketChannel serverSock) throws IOException {
        SocketChannel clientChan = serverSock.accept();
        if (clientChan == null)
            return;

        clientChan.configureBlocking(false);
        StompMessagingProtocol<T> protocol = protocolFactory.get();
        NonBlockingConnectionHandler<T> handler = new NonBlockingConnectionHandler<>(
                readerFactory.get(),
                protocol,
                clientChan,
                this);

        int connectionId = ((ConnectionsImpl<T>) connections).addConnection(handler);
        protocol.start(connectionId, connections);

        clientChan.register(selector, SelectionKey.OP_READ, handler);
    }

    private void handleReadWrite(SelectionKey key) {
        NonBlockingConnectionHandler<T> handler = (NonBlockingConnectionHandler<T>) key.attachment(); // get the handler

        if (key.isReadable()) {
            Runnable task = handler.continueRead();
            if (task != null)
                pool.submit(handler, task); // submit the read task to the thread pool
        }

        if (key.isValid() && key.isWritable()) {
            handler.continueWrite(); // continue writing to the channel
        }
    }

    private void runSelectionThreadTasks() { // run tasks queued for the selector thread
        while (!selectorTasks.isEmpty()) {
            selectorTasks.poll().run();
        }
    }

    public void updateInterestedOps(SocketChannel chan, int ops) {
        final SelectionKey key = chan.keyFor(selector);
        if (key == null)
            return;

        if (Thread.currentThread() == selectorThread) {
            if (key.isValid())
                key.interestOps(ops);
        } else {
            selectorTasks.add(() -> {
                if (key.isValid())
                    key.interestOps(ops);
            });
            selector.wakeup();
        }
    }

    @Override
    public void close() throws IOException {
        selector.close();
    }

    public Selector getSelector() {
        return selector;
    }
}
