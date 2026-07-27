#include "../include/StompProtocol.h"
#include <iostream>
#include <sstream>
#include <fstream>
#include <algorithm>
using namespace std;


StompProtocol::StompProtocol() : CH(nullptr), mutex_(), serverThread(), isConnected(false),
		shouldTerminate1(false), receiptCounter(1), username(""), subscriptionCounter(1),receiptAction(), topicToSubscriptionId(), gameEvents(){
		}
StompProtocol::~StompProtocol(){
	lock_guard<mutex> lock(mutex_);
	shouldTerminate1 = true;
	if (serverThread.joinable()){ // check if thread was started
		serverThread.join();
	}
	if (CH){
		CH->close();
		delete CH;
		CH = nullptr;
	}
}

void StompProtocol::start(){
	serverThread = thread([this]() { // thread to handle server messages
		while (!shouldTerminate()){
			string frame;
			if (CH && CH->getFrameAscii(frame, '\0')){
				handleServerFrame(frame);
			}
		}	
	});
	
	while (!shouldTerminate()){ // main thread to handle user input
		string line;
		getline(cin, line); // read user input
		istringstream iss(line); // parse input
		string command;
		iss >> command; // get command
		if (command == "login"){
			handleLogin(iss);
		}
		else if (command == "join"){
			handleJoin(iss);
		}
		else if (command == "exit"){
			handleExit(iss);
		}
		else if (command == "report"){
			handleReport(iss);
		}
		else if (command == "summary"){
			handleSummary(iss);
		}
		else if (command == "logout"){
			handleLogout();
		}
	}

	
}

void StompProtocol::handleLogin(istringstream &iss){
	lock_guard<mutex> lock(mutex_);
	if (isConnected){
		cout << "The client is already logged in, log out before trying again" << std::endl;
	}
	else{
		string hostPort, password;
		iss >> hostPort >> username >> password; 
		size_t colonPos = hostPort.find(':');
        string host = hostPort.substr(0, colonPos); // extract host
        short port = stoi(hostPort.substr(colonPos + 1)); // extract port
		CH = new ConnectionHandler(host, port); 
		if (!CH->connect()){ 
			cout << "Could not connect to server" << endl;
			delete CH; 
			CH = nullptr;
			return;
		}
		string msgLogin = "CONNECT\naccept-version:1.2\nhost:" + host + "\nlogin:" + username + "\npasscode:" + password +"\n\n\0";
		CH->sendLine(msgLogin); // send login frame to server
	}
}

void StompProtocol::handleJoin(istringstream &iss){
	if (isConnected){
		lock_guard<mutex> lock(mutex_);
		string game_name;
		iss >> game_name;
		string msgJoin = "SUBSCRIBE\ndestination:" + game_name + "\nid:" + to_string(subscriptionCounter) + "\nreceipt:" + to_string(receiptCounter) +"\n\n\0";
		topicToSubscriptionId[game_name] = subscriptionCounter; // map topic to subscription id
		receiptAction[receiptCounter] = "SUBSCRIBE" + game_name; // map receipt id to action
		subscriptionCounter++;
		receiptCounter++;
		CH->sendLine(msgJoin); // send subscribe frame to server
	}
}

void StompProtocol::handleExit(istringstream &iss){
	if (isConnected){
		lock_guard<mutex> lock(mutex_);
		string game_name;
		iss >> game_name;

		int subId = topicToSubscriptionId[game_name];
		if (subId == 0){
			cout << "You are not subscribed to this channel" << endl;
			return;
		}
		string msgExit = "UNSUBSCRIBE\nid:" + to_string(subId) + "\nreceipt:" + to_string(receiptCounter) +"\n\n\0";
		receiptAction[receiptCounter] = "UNSUBSCRIBE" + game_name; // map receipt id to action
		receiptCounter++;
		CH->sendLine(msgExit);
	}
}

void StompProtocol::handleReport(istringstream &iss){
	if (isConnected){
		lock_guard<mutex> lock(mutex_);
		string file;
		iss >> file;
		names_and_events nae = parseEventsFile(file);
		vector<Event> events = nae.events;
		sort(events.begin(), events.end(), [](const Event &a, const Event &b) { // sort events by time
			return a.get_time() < b.get_time();
		});
		for (Event ev : events){ // for each event
			gameEvents[nae.team_a_name + "_" + nae.team_b_name][username].push_back(ev);
			string msgReport = "SEND\ndestination:" + nae.team_a_name + "_" + nae.team_b_name + "\nfile:" + file +"\n\nuser:" + username + "\nteam a:"
			+ ev.get_team_a_name() + "\nteam b:" + ev.get_team_b_name() + "\nevent name:" + ev.get_name() + "\ntime:" + to_string(ev.get_time())
			+ "\ngeneral game updates:";
			for (auto& pair : ev.get_game_updates()){ // add general updates
				msgReport += "\n" + pair.first + ":" + pair.second;
			}
			msgReport += "\nteam a updates:";
			for (auto& pair : ev.get_team_a_updates()){
				msgReport += "\n" + pair.first + ":" + pair.second;
			}
			msgReport += "\nteam b updates:";
			for (auto& pair : ev.get_team_b_updates()){
				msgReport += "\n" + pair.first + ":" + pair.second;
			}
			msgReport += "\ndescription:\n" + ev.get_discription() + "\n\0";
			CH->sendLine(msgReport);
		}
	}
	

}

void StompProtocol::handleSummary(istringstream &iss){
	lock_guard<mutex> lock(mutex_);
	string game_name, user, file;
	iss >> game_name >> user >> file;
	ofstream outputFile(file);
	auto& data = gameEvents[game_name][user];
	map<string,string> generalStats, teamAStats, teamBStats;
	for (const Event& e : data) { // collect stats
    	for (auto& p : e.get_game_updates())
        	generalStats[p.first] = p.second;

    	for (auto& p : e.get_team_a_updates())
        	teamAStats[p.first] = p.second;

    	for (auto& p : e.get_team_b_updates())
        	teamBStats[p.first] = p.second;
	}	
	int signIndex = game_name.find("_");
	string teamAName = game_name.substr(0,signIndex);
	string teamBName = game_name.substr(signIndex + 1);
	outputFile << teamAName + " vs " + teamBName + "\n";
	
	outputFile << "Game stats:\n General stats:\n";
	for (auto& pair : generalStats){
		outputFile << pair.first + ":" + pair.second + "\n";
	}
	outputFile << "\n" + teamAName + " stats:\n";
	for (auto& pair : teamAStats){
		outputFile << pair.first + ":" + pair.second + "\n";
	}
	outputFile << "\n" + teamBName + " stats:\n";
	for (auto& pair : teamBStats){
		outputFile << pair.first + ":" + pair.second + "\n";
	}

	outputFile << "\nGame event reports:\n";
	for (Event ev : data){
		outputFile << to_string(ev.get_time()) + " - " + ev.get_name() + ":\n\n" + ev.get_discription() + "\n\n\n";
	}
	
}

void StompProtocol::handleLogout(){
	if (isConnected){
		lock_guard<mutex> lock(mutex_);
		string frame = "DISCONNECT\nreceipt:" + to_string(receiptCounter) + "\n\n\0";
		receiptAction[receiptCounter] = "DISCONNECT"; // map receipt id to action
		receiptCounter++;
		CH->sendLine(frame);
	}
	
}

void StompProtocol::handleServerFrame(const std::string& frame){
	lock_guard<mutex> lock(mutex_);
	if (frame.find("CONNECTED") == 0){ // handle CONNECTED frame
		isConnected = true;
		cout << "Login successful" << endl;
	}
	else if (frame.find("RECEIPT") == 0){
		size_t receiptIdPos = frame.find("receipt-id:"); // find receipt-id
        int receipt = stoi(frame.substr(receiptIdPos + 11));
		string action = receiptAction[receipt];
		if (action == "DISCONNECT"){
			isConnected = false;
			topicToSubscriptionId.clear();
			username.clear();
			CH->close();
			delete CH;
			CH = nullptr;
		}
		else if (action.find("SUBSCRIBE") == 0){
			cout << "Joined channel " + action.substr(9) << endl;
		}
		else if (action.find("UNSUBSCRIBE") == 0){
			cout << "Exited channel " + action.substr(11) << endl;
		}
			
	}
	else if (frame.find("MESSAGE") == 0){
		handleMessage(frame);
	}
	else if (frame.find("ERROR") == 0){
		handleError(frame);
		isConnected = false;
		topicToSubscriptionId.clear();
		if (CH){
			CH->close();
			delete CH;
			CH = nullptr;
		}
	}
}

void StompProtocol::handleError(const string& frame){
	if (frame.find("ALREADY_LOGGED_IN") != string::npos){
		cout << "User already logged in" << endl;
	}
	else if (frame.find("WRONG_PASSWORD") != string::npos){
		cout << "Wrong password" << endl;
	}
	else{
		cout << "An error occurred: " << frame << endl;
	}
}

void StompProtocol::handleMessage(const string& frame){
	size_t bodyPos = frame.find("\n\n"); // find body
	string header = frame.substr(0, bodyPos); // extract header
	string body = frame.substr(bodyPos + 2);
	string des;
	istringstream iss(header);
	string line;
	while (getline(iss, line)){ // parse header
		if (line.find("destination:") == 0){
			des = line.substr(12);
		}
	}
	Event ev(body); // create event from body
	gameEvents[des][username].push_back(ev); // store event
}

bool StompProtocol::shouldTerminate(){
	return shouldTerminate1;
}