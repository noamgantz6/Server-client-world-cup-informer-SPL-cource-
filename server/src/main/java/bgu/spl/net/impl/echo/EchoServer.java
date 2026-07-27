package bgu.spl.net.impl.echo;

import bgu.spl.net.srv.Server;

public class EchoServer {

    public static void main(String[] args) {

        // you can use any server... 
        //Server.threadPerClient(
         //      7777, //port
          //      EchoStompProtocol::new, //protocol factory
           //     LineMessageEncoderDecoder::new //message encoder decoder factory
        //).serve();

         Server.reactor(
                Runtime.getRuntime().availableProcessors(),
                7778, //port
             EchoStompProtocol::new, //protocol factory
                 LineMessageEncoderDecoder::new //message encoder decoder factory
        ).serve();
    }
}
