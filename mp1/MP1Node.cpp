/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
        
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);
    // \failedMemberList.clear();

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
        // size_t msgsize = sizeof(MessageHdr) + sizeof(joinaddr->addr) + sizeof(long) + 1;
        // msg = (MessageHdr *) malloc(msgsize * sizeof(char));
        // create JOINREQ message: format of data is {struct Address myaddr}
        msg = createMessage(JOINREQ);
        // memmove((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
        // memmove((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));

#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, sizeof(MessageHdr));

        free(msg);
    }

    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}



/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MessageHdr *msg = (MessageHdr *)data;
    //  Member *memberNode = (Member *)env;

    if (msg->msgType == JOINREQ) {
        // Info extracted as per message format in introduceSelfToGroup
        Address *recvAddr = &msg->addr;
		// long recvHeartbeat = msg->heartbeat;

        // Add new node to membership list
        updateList(msg->memberTable, msg->memberTableSize);
        
        // Send a copy of membership list to new node
        MessageHdr *repMsg = createMessage(JOINREP);
        emulNet->ENsend(&memberNode->addr, recvAddr, (char *)repMsg, sizeof(MessageHdr));
        cout << "send [" << par->getcurrtime() << "] JOINREP [" << (int) memberNode->addr.addr[0] << "] to [" << (int) msg->addr.addr[0] << "]" << endl;

        delete repMsg;
    }
    else if (msg->msgType == JOINREP) {
        memberNode->inGroup = true;
        // printAddress(&memberNode->addr);
        updateList(msg->memberTable, msg->memberTableSize);
        //cout << endl;
        cout << "receive [" << par->getcurrtime() << "] JOINREP [" <<  (int) memberNode->addr.addr[0] << "] from [" << (int) msg->addr.addr[0] << "]" << endl;
    }
    else if (msg->msgType == GOSSIP) {
        // Update entry of sender in member table
        updateList(msg->memberTable, msg->memberTableSize);
        cout << "receive [" << par->getcurrtime() << "] GOSSIP [" <<  (int) memberNode->addr.addr[0] << "] from [" << (int) msg->addr.addr[0] << "]" << endl;

    }
}

void MP1Node::updateList(MemberListEntry *MemberTable, int memberTableSize) {
    vector<MemberListEntry>::iterator itr;
    for (int i = 0; i < memberTableSize; i++) {
        if (MemberTable[i].heartbeat < 0) continue;
        for (itr = memberNode->memberList.begin(); itr != memberNode->memberList.end(); ++itr) {
            if (itr->id == (int) MemberTable[i].id && itr->port == (short) MemberTable[i].port) {
                if (itr->heartbeat >= 0 && itr->heartbeat < MemberTable[i].heartbeat) {
                    // cout << "[" << (int) memberNode->addr.addr[0] << "] updating heartbeat at [" << par->getcurrtime() << "] for [" << (int) MemberTable[i].id << "]" << endl;
                    itr->heartbeat = MemberTable[i].heartbeat;
                    itr->timestamp = par->getcurrtime();
                }
                break;
            }
        }
        if (itr == memberNode->memberList.end()) {
            MemberListEntry *entry = new MemberListEntry(
                MemberTable[i].id,
                MemberTable[i].port,
                MemberTable[i].heartbeat,
                par->getcurrtime()
            );
            memberNode->memberList.push_back(*entry);
            
            Address *newAddr = new Address();
            memset(newAddr->addr, 0, sizeof(newAddr->addr));
            newAddr->addr[0] = entry->id;
            newAddr->addr[4] = entry->port;
            cout << "[" << MemberTable[i].id << "] added to [" << (int) memberNode->addr.addr[0] << "]'s table" << endl;
            // printAddress(newAddr);
            log->logNodeAdd(&memberNode->addr, newAddr);
            free(entry);
            free(newAddr);
        }
    }
    /*cout << "[" << (int) memberNode->addr.addr[0] << "]'s table at [" << par->getcurrtime() << "]" << endl;
    for (auto entry : memberNode->memberList) {
        cout << entry.id << " " << entry.heartbeat << " " << entry.timestamp << endl;
    }*/
    return;
}

/*vector<MemberListEntry>::iterator MP1Node::findEntry(Address *addr) {
    int id = (int) addr->addr[0];
    int port = (short) addr->addr[4];

    for (auto itr = memberNode->memberList.begin(); itr != memberNode->memberList.end(); ++itr) 
        if (itr->id == id && itr->port == port) 
            return itr;

    return memberNode->memberList.end();
}

vector<MemberListEntry>::iterator MP1Node::findEntry(vector<MemberListEntry> &memberList, MemberListEntry entry) {
    for (auto itr = memberList.begin(); itr != memberList.end(); ++itr) 
        if (itr->id == entry.id && itr->port == entry.port) 
            return itr;

    return memberNode->memberList.end();
}*/

MessageHdr * MP1Node::createMessage(MsgTypes t) {
    MessageHdr *msg = new MessageHdr();
    msg->msgType = t;
    msg->addr = memberNode->addr;
    msg->memberTableSize = memberNode->memberList.size();
    msg->memberTable = new MemberListEntry[msg->memberTableSize];
    memmove(msg->memberTable, memberNode->memberList.data(), sizeof(MemberListEntry) * msg->memberTableSize);
    return msg;
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {
    // cout << "[" << (int) memberNode->addr.addr[0] << "] in nodeLoopOps at [" << par->getcurrtime() << "]" << endl;
    memberNode->heartbeat += 1;
    // Update self entry in membership table
    vector <MemberListEntry>::iterator it;
    for (it = memberNode->memberList.begin(); it != memberNode->memberList.end(); ++it) {
        if (it->id == (int) memberNode->addr.addr[0] && it->port == (short) memberNode->addr.addr[4]) {
            // cout << "[" << (int) memberNode->addr.addr[0] << "] updating heartbeat at [" << par->getcurrtime() << "]" << endl;
            it->heartbeat = memberNode->heartbeat;
            it->timestamp = par->getcurrtime();
        }
        break;
        //cout << "Updating "
    }

    // Check membership status - move suspected failed node to failed list
    for (auto itr = memberNode->memberList.begin(); itr != memberNode->memberList.end();) {
        if (par->getcurrtime() - itr->timestamp >= TFAIL) {    
            itr->heartbeat = -1;
        }
        if (par->getcurrtime() - itr->timestamp >= TREMOVE) {
            cout << "[" << itr->id << "]" << "removed from [" << (int) memberNode->addr.addr[0] << "] at [" << par->getcurrtime() << "]" << endl;
            Address *addrDeleted = new Address();
            memset(addrDeleted->addr, 0, sizeof(addrDeleted->addr));
            addrDeleted->addr[0] = itr->id;
            addrDeleted->addr[4] = itr->port;
            itr = memberNode->memberList.erase(itr);
            // printAddress(addrDeleted);
            log->logNodeRemove(&memberNode->addr, addrDeleted);
            free(addrDeleted);
        }
        else ++itr;
    }

    // Send gossip message
    int span = 4;
    while (span--) {
        int sendTo;
        do {
            sendTo = rand() % memberNode->memberList.size();
        } while (memberNode->memberList[sendTo].heartbeat < 0 && 
                 memberNode->memberList[sendTo].id != memberNode->addr.addr[0]);
    // for (int sendTo = 0; sendTo < memberNode->memberList.size(); sendTo++) {
        //if (memberNode->memberList[sendTo].heartbeat >= 0 && memberNode->memberList[sendTo].id != (int) memberNode->addr.addr[0]) {   
        MessageHdr *msg = createMessage(GOSSIP);
        Address *newAddr = new Address();
        memset(newAddr->addr, 0, sizeof(newAddr->addr));
        newAddr->addr[0] = memberNode->memberList[sendTo].id;
        newAddr->addr[4] = memberNode->memberList[sendTo].port;
        cout << "send [" << par->getcurrtime() << "] GOSSIP [" << (int) memberNode->addr.addr[0] << "] to [" << (int) newAddr->addr[0] << "]" << endl;

        emulNet->ENsend(&memberNode->addr, newAddr, (char *) msg, sizeof(MessageHdr));
        free(msg);
        free(newAddr);
    //    }
    }
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
    MemberListEntry *entry = new MemberListEntry((int) memberNode->addr.addr[0], 
                                                 (short) memberNode->addr.addr[4],
                                                 memberNode->heartbeat,
                                                 par->getcurrtime());
    memberNode->memberList.push_back(*entry);
    log->logNodeAdd(&memberNode->addr, &memberNode->addr);

}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}
