package bgu.spl.net.impl.stomp;

import bgu.spl.net.api.StompMessagingProtocol;
import bgu.spl.net.srv.Connections;
import bgu.spl.net.impl.data.Database;
import bgu.spl.net.impl.data.LoginStatus;

import java.util.HashMap;
import java.util.Map;

public class StompMessagingProtocolImpl<T> implements StompMessagingProtocol<T> {

    private int connectionId;
    private Connections<T> connections;
    private boolean shouldTerminate = false;
    private final Database db = Database.getInstance();
    private int messageIdCounter = 1;

    @Override
    public void start(int connectionId, Connections<T> connections) {
        this.connectionId = connectionId;
        this.connections = connections;
    }

    @Override
    public void process(T message) {
        if (message == null) {
            return;
        }
        String messageStr = message.toString();
        String[] parts = messageStr.split("\n\n", 2); // split headers and body
        String headerPart = parts.length > 0 ? parts[0] : ""; // headers
        String body = parts.length > 1 ? parts[1] : ""; // body
        String[] lines = headerPart.split("\n");
        if (lines.length == 0) {
            return;
        }
        String command = lines[0].trim(); // first line is command
        Map<String, String> headers = new HashMap<>(); // headers map
        for (int i = 1; i < lines.length; i++) {
            String[] kv = lines[i].split(":", 2); // split key and value
            if (kv.length == 2) {
                headers.put(kv[0].trim(), kv[1].trim());
            }
        }

        switch (command) { // handle commands
            case "CONNECT":
                handleConnect(headers);
                break;
            case "SUBSCRIBE":
                handleSubscribe(headers, body);
                break;
            case "UNSUBSCRIBE":
                handleUnsubscribe(headers, body);
                break;
            case "SEND":
                handleSend(headers, body);
                break;
            case "DISCONNECT":
                handleDisconnect(headers, body);
                break;
            default:
                sendError("Unknown command: " + command);
                break;
        }
    }

    private void handleConnect(Map<String, String> headers) {
        if (headers.get("accept-version") == null) { // check required headers
            sendError("Missing accept-version header");
            return;
        }
        if (headers.get("host") == null) {
            sendError("Missing host header");
            return;
        }
        String username = headers.get("login"); // get username
        String pass = headers.get("passcode"); // get password
        if (username == null || pass == null) {
            sendError("Missing login or passcode");
            return;
        }
        if (username.isEmpty() || pass.isEmpty()) {
            sendError("Empty login or passcode");
            return;
        }
        LoginStatus status = db.login(connectionId, username, pass); // try to login
        if (status == LoginStatus.LOGGED_IN_SUCCESSFULLY || status == LoginStatus.ADDED_NEW_USER) { // if success
            String connected = "CONNECTED\nversion:1.2\n\n\u0000";
            connections.send(connectionId, (T) connected); // send CONNECTED frame
        } else {
            sendError("Login failed: " + status.name()); // send ERROR frame
        }
    }

    private void handleSubscribe(Map<String, String> headers, String body) {
        if (body != null && !body.isEmpty()) { // check no body
            sendError("SUBSCRIBE frame should not have a body");
            return;
        }
        String dest = headers.get("destination");
        String id = headers.get("id");
        String receipt = headers.get("receipt");
        if (dest == null || id == null) { // check required headers
            sendError("Missing destination or id header");
            return;
        }
        try {
            int subId = Integer.parseInt(id); // move to integer
            ((ConnectionsImpl<T>) connections).subscribe(connectionId, dest, subId); // subscribe to topic
            sendReceipt(receipt); // send RECEIPT frame
        } catch (NumberFormatException e) { // invalid id
            sendError("Invalid subscription id: " + id);
        }
    }

    private void handleUnsubscribe(Map<String, String> headers, String body) {
        if (body != null && !body.isEmpty()) { // check no body
            sendError("UNSUBSCRIBE frame should not have a body");
            return;
        }
        String id = headers.get("id");
        if (id == null) { // check required header
            sendError("Missing id header");
            return;
        }
        String receipt = headers.get("receipt");
        try {
            int subId = Integer.parseInt(id);
            ((ConnectionsImpl<T>) connections).unsubscribe(connectionId, subId); // unsubscribe
            sendReceipt(receipt);
        } catch (NumberFormatException e) {
            sendError("Invalid subscription id: " + id);
            return;
        }
    }

    private void handleSend(Map<String, String> headers, String body) {
        String dest = headers.get("destination");
        if (dest == null) { // check required header
            sendError("Missing destination header");
            return;
        }
        if (!((ConnectionsImpl<T>) connections).isSubscribed(connectionId, dest)) { // check subscription
            sendError("Not subscribed to destination");
            return;
        }
        String frame = "MESSAGE\nsubscription:"
                + ((ConnectionsImpl<T>) connections).getSubscriptionId(connectionId, dest)
                + "\nmessage-id:" + messageIdCounter + "\ndestination:" + dest + "\n\n" + body + "\u0000";
        messageIdCounter++;
        ((ConnectionsImpl<T>) connections).send(dest, (T) frame); // send to topic subscribers
        String username = db.getUsernameByConnectionId(connectionId);
        String file = headers.get("file");
        db.trackFileUpload(username, file, dest); // log file upload
    }

    private void handleDisconnect(Map<String, String> headers, String body) {
        if (body != null && !body.isEmpty()) { // check no body
            sendError("DISCONNECT frame should not have a body");
            return;
        }
        String receipt = headers.get("receipt");
        if (receipt == null) { // check required header
            sendError("Missing receipt header");
            return;
        }
        shouldTerminate = true;
        sendReceipt(receipt);
        connections.disconnect(connectionId);
        db.logout(connectionId);
    }

    private void sendError(String msg) {
        String frame = "ERROR\nmessage: " + msg + "\n\n\u0000";
        connections.send(connectionId, (T) frame);
        connections.disconnect(connectionId);
        db.logout(connectionId);
    }

    private void sendReceipt(String receiptId) {
        String frame = "RECEIPT\nreceipt-id:" + receiptId + "\n\n\u0000";
        connections.send(connectionId, (T) frame);
    }

    @Override
    public boolean shouldTerminate() {
        return shouldTerminate;
    }

}
