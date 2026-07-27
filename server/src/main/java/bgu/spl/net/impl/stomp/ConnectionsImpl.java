package bgu.spl.net.impl.stomp;

import bgu.spl.net.srv.ConnectionHandler;
import bgu.spl.net.srv.Connections;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class ConnectionsImpl<T> implements Connections<T> {
	private final ConcurrentMap<Integer, ConnectionHandler<T>> connections = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, ConcurrentMap<Integer, Integer>> topicSubscribers = new ConcurrentHashMap<>();
	private final ConcurrentMap<Integer, ConcurrentMap<Integer, String>> subscriptions = new ConcurrentHashMap<>();

	private final AtomicInteger idCounter = new AtomicInteger(1); // unique connection IDs starting from 1

	@Override
	public boolean send(int connectionId, T msg) {
		ConnectionHandler<T> handler = connections.get(connectionId); // the handler
		if (handler != null) {
			try {
				handler.send(msg); // send the message
				return true;
			} catch (Exception e) {
				return false;
			}
		}
		return false;
	}

	@Override
	public void send(String channel, T msg) {
		ConcurrentMap<Integer, Integer> subs = topicSubscribers.get(channel);
		if (subs == null) { // no subscriber
			return;
		}
		for (Integer connectionId : subs.keySet()) { // for each subscriber
			send(connectionId, msg);
		}
	}

	@Override
	public void disconnect(int connectionId) {
		ConnectionHandler<T> handler = connections.remove(connectionId); // remove the handler
		ConcurrentMap<Integer, String> subs = subscriptions.remove(connectionId); // remove subscriptions
		if (subs != null) {
			for (String topic : subs.values()) { // for each subscribed topic
				ConcurrentMap<Integer, Integer> map = topicSubscribers.get(topic); // the topic's subscribers
				if (map != null) {
					map.remove(connectionId); // remove the connection from the topic's subscribers
					if (map.isEmpty()) {
						topicSubscribers.remove(topic); // remove topic if no subscribers
					}
				}
			}
		}
	}

	public int addConnection(ConnectionHandler<T> handler) {
		int id = idCounter.getAndIncrement(); // get unique id
		connections.put(id, handler);
		subscriptions.put(id, new ConcurrentHashMap<>());
		return id;
	}

	public void subscribe(int connectionId, String topic, int subscriptionId) {
		subscriptions.putIfAbsent(connectionId, new ConcurrentHashMap<>()); // create connection subscriptions if not
																			// exists
		if (isSubscribed(connectionId, topic)) {
			return; // already subscribed
		}
		subscriptions.get(connectionId).put(subscriptionId, topic); // add subscription
		topicSubscribers.computeIfAbsent(topic, k -> new ConcurrentHashMap<>()).put(connectionId, subscriptionId); // add
																													// connection
																													// to
																													// topic
																													// subscribers
																													// if
																													// not
																													// exists
	}

	public void unsubscribe(int connectionId, int subscriptionId) {
		ConcurrentMap<Integer, String> subs = subscriptions.get(connectionId); // the connection subscriptions
		if (subs == null) { // no subscriptions
			return;
		}
		String topic = subs.remove(subscriptionId); // remove the subscription
		if (topic == null) {
			return;
		}
		ConcurrentMap<Integer, Integer> map = topicSubscribers.get(topic); // the topic's subscribers
		if (map != null) { // remove the connection
			map.remove(connectionId);
			if (map.isEmpty()) {
				topicSubscribers.remove(topic); // remove topic if no subscribers
			}
		}
	}

	public boolean isSubscribed(int connectionId, String topic) {
		ConcurrentMap<Integer, Integer> subs = topicSubscribers.get(topic);
		return subs != null && subs.containsKey(connectionId);
	}

	public int getSubscriptionId(int connectionId, String topic) {
		ConcurrentMap<Integer, Integer> subs = topicSubscribers.get(topic);
		if (subs != null) {
			return subs.getOrDefault(connectionId, -1);
		}
		return -1;
	}

}
