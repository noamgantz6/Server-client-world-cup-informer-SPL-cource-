#pragma once

#include "../include/ConnectionHandler.h"
#include <event.h>
#include <map>
#include <string>
#include<thread>
#include <mutex>

// TODO: implement the STOMP protocol
class StompProtocol
{
private:
    ConnectionHandler* CH;    
    std::mutex mutex_;
    std::thread serverThread;
    bool isConnected; 
    bool shouldTerminate1; 
    int receiptCounter; 
    std::string username;
    int subscriptionCounter; 
    std::map<int, std::string> receiptAction; 
    std::map<std::string, int> topicToSubscriptionId; 
    std::map<std::string, std::map<std::string,std::vector<Event>>> gameEvents;
    void handleLogin(std::istringstream &iss);
    void handleJoin(std::istringstream &iss);
    void handleExit(std::istringstream &iss);
    void handleReport(std::istringstream &iss);
    void handleSummary(std::istringstream &iss);
    void handleServerFrame(const std::string& frame);
    void handleLogout();
    void handleError(const std::string& frame);
    void handleMessage(const std::string& frame);
public:
    StompProtocol();
    ~StompProtocol();
    StompProtocol(const StompProtocol&) = delete;
    StompProtocol& operator=(const StompProtocol&) = delete;
    void start();
    bool shouldTerminate();

    
};