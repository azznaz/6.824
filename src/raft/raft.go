package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	//  "sort"
	"fmt"
	"math/rand"
	"sync"
	"time"
)
import "sync/atomic"
import "../labrpc"
import "../labgob"
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}
type LogEntry struct {
	Command interface{}
	Term    int
	Index int
}
type State int

const (leader = 0
	candidate = 1
	follower = 2
)
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu           sync.Mutex          // Lock to protect shared access to this peer's state
	peers       []*labrpc.ClientEnd // RPC end points of all peers
	persister   *Persister          // Object to hold this peer's persisted state
	me          int                 // this peer's index into peers[]
	dead        int32               // set by Kill()
	state       State
	lastReceive time.Time
	electionOUT time.Duration
	votedFor    int
	logs        []LogEntry
	currentTerm int
	commitIndex int
	lastApplied int
	nextIndex []int
	matchIndex []int
	flag        time.Time
	applyCh chan ApplyMsg
	termToindex map[int]int
	snapShotIndex int
	snapShotTerm int
	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}
func (rf *Raft) random()time.Duration{
	rand.Seed(time.Now().UnixNano()*int64((rf.me*10+5)*100))
	return time.Duration(int64(300 + rand.Intn(200)))*time.Millisecond
}
func (rf *Raft) reSetTime(){
	rf.lastReceive = time.Now()
	rf.electionOUT = rf.random()
}
func (rf *Raft) logLen() int {
	return  len(rf.logs)
}
func (rf *Raft) getLogEntry(index int)LogEntry {
	//fmt.Printf("server %d index is %d snappshotindex is %d\n",rf.me,index,rf.snapShotIndex)
	if index <= 0{
		return LogEntry{Index: 0,Term: -1}
	}
	return rf.logs[index-rf.snapShotIndex-1]
}
func (rf *Raft) setLogEntry(index int,log LogEntry)  {
	rf.logs[index-rf.snapShotIndex-1]=log
}
func (rf *Raft) getLastlogIndex() int {
	return rf.snapShotIndex + rf.logLen()
}
func (rf *Raft) getLastlogTerm() int{
	index := rf.getLastlogIndex()
	if index == 0{
		return -1
	}
	if index == rf.snapShotIndex{
		return rf.snapShotTerm
	}
	return rf.getLogEntry(index).Term
}
func (rf *Raft)isUpToDate(args *RequestVoteArgs)bool  {
	if args.LastLogIndex == 0 && rf.getLastlogIndex() == 0{
		return true
	}
	if args.LastLogIndex == 0 && rf.getLastlogIndex() != 0{
		return  false
	}
	if args.LastLogIndex != 0 && rf.getLastlogIndex() == 0{
		return  true
	}
	if args.LastLogTerm > rf.getLastlogTerm() {
		return  true
	}else if args.LastLogTerm == rf.getLastlogTerm() && args.LastLogIndex >= rf.getLastlogIndex() {
		return true
	}
	return  false
}
func (rf *Raft) min(x,y int) int{
	if x >= y {
		return  y
	}
	return  x
}
func (rf *Raft) max(x ,y int) int {
	if x >= y {
		return  x
	}
	return  y
}
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer  rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.snapShotIndex)
	e.Encode(rf.snapShotTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}
func (rf *Raft)GetRaftSize() int{
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var preTerm int
	var preVote int
	var preLogs []LogEntry
	var preSnapShotIndex int
	var preSnapShotTerm int
	if d.Decode(&preTerm) !=nil || d.Decode(&preVote)!= nil || d.Decode(&preLogs) !=nil || d.Decode(&preSnapShotIndex)!=nil || d.Decode(&preSnapShotTerm)!=nil{
		fmt.Printf("readPersist error!\n")
	}else {
		rf.currentTerm = preTerm
		rf.votedFor = preVote
		rf.logs = preLogs
		rf.snapShotIndex = preSnapShotIndex
		rf.snapShotTerm = preSnapShotTerm
	}
}

type InstallSnapshotArgs struct {
	Term int
	LeaderId int
	LastIncludedIndex int
	LastIncludedTerm int
	Snapshot []byte
}

type InstallSnapshotReply struct {
	Term int
}
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex >= lastIncludedIndex || rf.snapShotIndex >= lastIncludedIndex{
	//	fmt.Printf("server %d judge if install snapshot : no,commitindex is %d lastincludedindex is %d\n",rf.me,rf.commitIndex,lastIncludedIndex)
		return false
	}
	//fmt.Printf("server %d judge if install snapshot : yes,snapshotidnex is %d snapshotterm is%d\n",rf.me,lastIncludedIndex,lastIncludedTerm)
	rf.snapShotTerm = lastIncludedTerm
	rf.snapShotIndex = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex
	rf.logs = make([]LogEntry,0)
	rf.termToindex = make(map[int]int)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.snapShotIndex)
	e.Encode(rf.snapShotTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data,snapshot)
	return true
}
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//fmt.Printf("server %d update snapshot: lastindex is %d index is %d " +
	//	"snapindex is %d\n",rf.me,rf.getLastlogIndex(),index,rf.snapShotIndex)
	num := rf.getLastlogIndex()-index
//	fmt.Printf("server %d index is %d snapshotindex is %d\n",rf.me,index,rf.snapShotIndex)
	var term int
	if index == rf.snapShotIndex{
		//fmt.Printf("server %d index is %d snapindex is %d\n",rf.me,index,rf.snapShotIndex)
		term = rf.snapShotTerm
		//return
	}else{
		term = rf.getLogEntry(index).Term
	}
	//term = rf.getLogEntry(index).Term
	arr := make([]LogEntry,num)
	for i := 0;i<num;i++{
		arr[i] = rf.getLogEntry(i+index+1)
	}
	newTermToIndex := make(map[int]int)

	for i:=0;i<num;i++{
		if newTermToIndex[arr[i].Term] == 0{
			newTermToIndex[arr[i].Term] = arr[i].Index
		}
	}
	rf.termToindex=newTermToIndex
	rf.logs = arr
	rf.snapShotIndex=index
	rf.snapShotTerm=term
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	e.Encode(rf.snapShotIndex)
	e.Encode(rf.snapShotTerm)
	data := w.Bytes()
	rf.persister.SaveStateAndSnapshot(data,snapshot)
}
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs,reply *InstallSnapshotReply){
	rf.mu.Lock()
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		rf.persist()
	}
	if args.Term >= rf.currentTerm {
		rf.reSetTime()
	}
	reply.Term=rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	snapMsg := ApplyMsg{CommandValid: false,SnapshotValid: true,SnapshotIndex: args.LastIncludedIndex,SnapshotTerm: args.LastIncludedTerm,Snapshot: args.Snapshot}
	rf.mu.Unlock()
	fmt.Printf("%d take snapMsg send to appylCh snapshotindex is %d leader is %d\n",rf.me,args.LastIncludedIndex,args.LeaderId)
	rf.applyCh<-snapMsg
}
//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
	LastLogIndex int
	LastLogTerm int

}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}
type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PreLogIndex int
	PreLogTerm int
	Entries  []LogEntry
	LeaderCommit int
}
type AppendEntriesReply struct {
	Term int
	Success bool
	FirstIndex int
}
//
// example RequestVote RPC handler.
//

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//have a bug
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		rf.persist()
			//reply.VoteGranted = true
			//rf.state = follower
			//rf.reSetTime()
			//rf.votedFor = args.CandidateId
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	}else if  rf.isUpToDate(args) && (rf.votedFor == -1 ||rf.votedFor==args.CandidateId){
		reply.VoteGranted = true
		//rf.state = follower
		rf.reSetTime()
		rf.votedFor = args.CandidateId
		rf.persist()

	}else {
		reply.VoteGranted = false
	}



	  //if reply.VoteGranted {
	  //      fmt.Printf("%d vote to %d at candidate %d term but itself at %d term\n",rf.me,args.CandidateId,args.Term,rf.currentTerm)
	  //  }else {
	  //  	flag1 := rf.isUpToDate(args)
	  //  	var s string
	  //  	if flag1{
	  //  		s = "uptodate"
		//	}else {
		//		s = "no uptodate"
		//	}
	  //
	  //    fmt.Printf("%d do not vote to %d at candidate %d term but itself at %d term and reason of no vote is %s votedFor is %d\n",rf.me,args.CandidateId,args.Term,rf.currentTerm,s,rf.votedFor)
	  //
	  //  }
}
func  (rf *Raft)isContains(args *AppendEntriesArgs)(int,bool) {

	if args.PreLogIndex <= 0 || args.PreLogIndex <= rf.snapShotIndex{
		return  0,true
	}
	if rf.getLastlogIndex()== 0{
		return 0,false
	}
	if args.PreLogIndex > rf.getLastlogIndex() {
		if rf.getLastlogIndex() == rf.snapShotIndex{

			return rf.snapShotIndex+1,false
		}
		if rf.termToindex[rf.getLastlogTerm()] > rf.getLastlogIndex(){
			//fmt.Printf("error:%d isContains1 termToindex %d > lastindex %d term is %d\n",rf.me,rf.termToindex[rf.getLastlogTerm()],rf.getLastlogIndex(),rf.getLastlogTerm())
		}
		return rf.termToindex[rf.getLastlogTerm()],false
	}
	if  rf.getLogEntry(args.PreLogIndex).Term != args.PreLogTerm{
		if rf.termToindex[rf.getLogEntry(args.PreLogIndex).Term] > args.PreLogIndex {
			//fmt.Printf("error:%d isContains2 termToindex %d > preLogindex %d term is %d\n",rf.me,rf.termToindex[rf.getLogEntry(args.PreLogIndex).Term],args.PreLogIndex,rf.getLogEntry(args.PreLogIndex).Term)
		}
		return rf.termToindex[rf.getLogEntry(args.PreLogIndex).Term],false
	}
	return  0,true
}
func (rf *Raft) AppendEntries(args *AppendEntriesArgs,reply *AppendEntriesReply)  {
	//fmt.Printf("%d Append Handle\n",rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term > rf.currentTerm {
		rf.convertToFollower(args.Term)
		rf.persist()
	}
	if args.Term >= rf.currentTerm {
		rf.reSetTime()
	}
	reply.Term = rf.currentTerm
	index,ok :=  rf.isContains(args)
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.FirstIndex = 0
		return
	}
	if !ok{
	//	fmt.Printf("%d receive AP from %d but not contains at leader %d term\n",rf.me,args.LeaderId,args.Term)
	}
	if ok {
		reply.Success = true
	//	var base int

		base := args.PreLogIndex+1
		lens := len(args.Entries)
		deIndex := rf.logLen()+1
		flag := false
		if lens > 0 {
			//fmt.Printf("%d add logs: %v \n and before adding %d logs is:%v\n",rf.me,args.Entries,rf.me,rf.logs)
		}
		logLen := rf.getLastlogIndex()
		for i:=base;i<=logLen;i++{
			if i <= rf.snapShotIndex{
				continue
			}
			if rf.termToindex[rf.getLogEntry(i).Term] == i || rf.termToindex[rf.getLogEntry(i).Term] > i{
				rf.termToindex[rf.getLogEntry(i).Term] = 0
				//fmt.Printf("clear: %d set term to index key:%d value:%d\n",rf.me,rf.getLogEntry(i).Term,0)
			}
		}
		for i := 0; i < lens;i++{
			if args.Entries[i].Index <= rf.snapShotIndex{
				continue
			}
			if base + i > rf.getLastlogIndex(){
				rf.logs = append(rf.logs,args.Entries[i])
				if rf.termToindex[args.Entries[i].Term] == 0{
					rf.termToindex[args.Entries[i].Term] = base + i
					//fmt.Printf("set: %d set term to index key:%d value:%d\n",rf.me,args.Entries[i].Term,base+i)
				}
				flag = false
				continue
			}
			if rf.getLogEntry(base+i).Term == args.Entries[i].Term {
				//rf.logs[base + i] = args.Entries[i]
				if rf.termToindex[args.Entries[i].Term] == 0{
					rf.termToindex[args.Entries[i].Term] = base + i
				//	fmt.Printf("set: %d set term to index key:%d value:%d\n",rf.me,args.Entries[i].Term,base+i)
				}
				rf.setLogEntry(base+i,args.Entries[i])
				continue
			}
			if rf.termToindex[rf.getLogEntry(base+i).Term] == base + i{
				rf.termToindex[rf.getLogEntry(base+i).Term] = 0
				//fmt.Printf("set: %d set term to index key:%d value:%d\n",rf.me,rf.getLogEntry(base+i).Term,0)
			}
			if rf.termToindex[args.Entries[i].Term] == 0 || rf.termToindex[args.Entries[i].Term] > base + i{
				rf.termToindex[args.Entries[i].Term] = base + i
			//	fmt.Printf("set: %d set term to index key:%d value:%d\n",rf.me,args.Entries[i].Term,base+i)
			}
			flag = true
			deIndex = base+i+1
			rf.setLogEntry(base+i,args.Entries[i])
			//rf.logs[base + i] = args.Entries[i]
			//rf.logs  = rf.logs[:base+i+1]

		}
		if flag {
			//fmt.Printf("delete: %d delete log from %d to %d \n",rf.me,deIndex,rf.getLastlogIndex())
			rf.logs = rf.logs[:deIndex-rf.snapShotIndex-1]
		}
		if lens > 0{
			rf.persist()
			//fmt.Printf("%d logindex from %d to %d at %d term leaderId is %d\n",rf.me,base-1,rf.getLastlogIndex(),rf.currentTerm,args.LeaderId)
			//fmt.Printf("after adding %d logs is:%v\n",rf.me,rf.logs)
		}
		if args.LeaderCommit > rf.commitIndex {
			pci := rf.commitIndex
			rf.commitIndex = rf.min(args.LeaderCommit,args.PreLogIndex+len(args.Entries))
			if pci != rf.commitIndex {
				//fmt.Printf("commitIndex of %d from %d to %d at %d term and leader is %d leaderCommit is %d\n ",rf.me,pci,rf.commitIndex,args.Term,args.LeaderId,args.LeaderCommit)
			}
		}
	}else {
		reply.Success = false
		reply.FirstIndex = index
	}
	if args.Term >= rf.currentTerm {
		rf.reSetTime()
	//	fmt.Printf("AP:%d reSetTime,lastReceive is %v and electionOUT is %v\n",rf.me,rf.lastReceive,rf.electionOUT)
	}
}
//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft)sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
func (rf *Raft)sendInstallSnapShot(server int,args *InstallSnapshotArgs,reply *InstallSnapshotReply)bool  {
	ok := rf.peers[server].Call("Raft.InstallSnapshot",args,reply)
	return ok
}


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader || rf.killed(){
		isLeader = false
		return index,term,isLeader
	}

	index = rf.getLastlogIndex()+1
	rf.logs = append(rf.logs,LogEntry{Command: command,Term: rf.currentTerm,Index: index})

	rf.matchIndex[rf.me]=index
	term = rf.currentTerm
	//fmt.Printf("%d add log %v at %d term by start index is %d\n",rf.me,command,rf.currentTerm,index)
	if rf.termToindex[rf.currentTerm] == 0 {
		rf.termToindex[rf.currentTerm]=index
	}
	rf.persist()
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(50*time.Millisecond)
		rf.mu.Lock()
		if rf.state == leader || time.Now().Sub(rf.lastReceive) < rf.electionOUT {
			rf.mu.Unlock()
			continue
		}
		rf.mu.Unlock()

		rf.attemptElection()
	}
}
func (rf *Raft) sendSnapShot(server int)  {
	snapShot := rf.persister.ReadSnapshot()
	args := InstallSnapshotArgs{Term: rf.currentTerm,LeaderId: rf.me,LastIncludedTerm: rf.snapShotTerm,LastIncludedIndex: rf.snapShotIndex,Snapshot: snapShot}
	reply := InstallSnapshotReply{}
	go func(server int,args InstallSnapshotArgs,reply InstallSnapshotReply) {
		rf.mu.Lock()
		if rf.currentTerm != args.Term || rf.state!=leader{
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		ok := rf.sendInstallSnapShot(server,&args,&reply)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if !ok{
			return
		}
		if rf.currentTerm != args.Term || rf.state!=leader{
			return
		}
		if reply.Term > rf.currentTerm {
			rf.convertToFollower(reply.Term)
			rf.persist()
			return
		}
		rf.nextIndex[server] = args.LastIncludedIndex+1
	}(server,args,reply)
}
func (rf *Raft) heartBeats(){
	for {
		rf.mu.Lock()
		if rf.killed(){
			rf.mu.Unlock()
			return
		}
		if rf.state != leader{
			rf.mu.Unlock()
			return
		}
		for i:=0;i<len(rf.peers);i++{
		//	fmt.Printf("i is %d me is %d\n",i,rf.me)
			if rf.me == i {
				continue
			}
			//fmt.Printf("%d send heartBeats to %d at %d term\n",rf.me,i,rf.currentTerm)
			if rf.nextIndex[i] <= rf.snapShotIndex{
				//fmt.Printf("server %d sendSnapshot to %d,nextindex is %d " +
				//	"snapshotIndex is %d i is %d me is %d\n",rf.me,i,rf.nextIndex[i],rf.snapShotIndex,i,rf.me)
				rf.sendSnapShot(i)
				//rf.nextIndex[i]=rf.snapShotIndex+1
				continue
			}
			num :=  rf.getLastlogIndex()-rf.nextIndex[i]+1
			if num < 0{
				//fmt.Printf("error: %d send to %d num is %d lastindex is %d nextindex is %d\n",rf.me,i,num,rf.getLastlogIndex(),rf.nextIndex[i])
			}
			logs := make([]LogEntry,num)
			for  j := 0;j<num;j++ {
				logs[j] = rf.getLogEntry(rf.nextIndex[i]+j)
			}
			if num>0{
				//fmt.Printf("%d send to %d log index from %d to %d and log is %v \n",rf.me,i,rf.nextIndex[i],rf.getLastlogIndex(),logs)
			}
			preIndex := rf.nextIndex[i]-1
			var preTerm int
			if preIndex == rf.snapShotIndex {
				preTerm = rf.snapShotTerm
			}else {
				//fmt.Printf("preIndex is %d\n",preIndex)
				preTerm = rf.getLogEntry(preIndex).Term
				//preTerm = rf.logs[preIndex].Term
			}
			args := AppendEntriesArgs{Term: rf.currentTerm,LeaderId: rf.me,PreLogIndex: preIndex,
				PreLogTerm: preTerm,Entries: logs,LeaderCommit: rf.commitIndex}
			reply := AppendEntriesReply{}
			next := rf.getLastlogIndex()+1

			go func(server int, args AppendEntriesArgs,reply AppendEntriesReply,next int) {
				rf.mu.Lock()
				if rf.currentTerm != args.Term || rf.state!=leader{
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()
				ok := rf.sendAppendEntries(server,&args,&reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if !ok {
					return
				}
				if rf.currentTerm != args.Term || rf.state!=leader{
					return
				}
				if reply.Term > rf.currentTerm {

					//fmt.Printf("heartBeats:because reply term %d change term from %d to %d\n",rf.me,rf.currentTerm,reply.Term)
					rf.convertToFollower(reply.Term)
					rf.persist()
					return
				}
				if reply.Success {
				//	rf.nextIndex[server] = rf.max(next,rf.nextIndex[server])
				//	fmt.Printf("Success %d to %d nextindex from %d to %d \n",rf.me,server,rf.nextIndex[server],next)
					rf.nextIndex[server] = next
					rf.matchIndex[server] =args.PreLogIndex + len(args.Entries)
					//fmt.Printf("%d to %d nextindex from %d to %d \n",rf.me,server,rf.nextIndex[server],next)
				}else {
				//	var pre int
				//	pre = rf.nextIndex[server]
					if rf.nextIndex[server] != 1 {
						if reply.FirstIndex == 0{
							rf.nextIndex[server] = 1
						}else {
							rf.nextIndex[server] = reply.FirstIndex
						}
					}
					//fmt.Printf("Fail %d to %d nextindex from %d to %d \n",rf.me,server,pre,rf.nextIndex[server])
				}
			}(i,args,reply,next)
		}
		rf.mu.Unlock()
		time.Sleep(90*time.Millisecond)
	}
}
func (rf *Raft) attemptElection() {

	rf.mu.Lock()
	rf.convertToCandidate()
	rf.reSetTime()
	//fmt.Printf("%d start election at %d term lastReceive is %v and electionOUT is %v\n",rf.me,rf.currentTerm,rf.lastReceive,rf.electionOUT)
	count := 1
	done := false
	for i := 0;i < len(rf.peers) ;i++{
		if rf.me == i {
			continue
		}
		args := RequestVoteArgs{Term: rf.currentTerm,CandidateId: rf.me,LastLogIndex: rf.getLastlogIndex(),LastLogTerm: rf.getLastlogTerm()}
		reply := RequestVoteReply{}
		go func(serve int,args RequestVoteArgs,reply RequestVoteReply) {
			rf.mu.Lock()
			if rf.currentTerm != args.Term || rf.state!=candidate{
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
		//	fmt.Printf("%d send request vote to %d at %d term\n",rf.me,serve,args.Term)
			ok := rf.sendRequestVote(serve,&args,&reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if !ok {
				return
			}
			if rf.currentTerm != args.Term || rf.state!=candidate{
				return
			}
			if reply.Term > rf.currentTerm {
				//fmt.Printf("election:because reply term %d change term from %d to %d\n",rf.me,rf.currentTerm,reply.Term)
				rf.convertToFollower(reply.Term)
				rf.persist()
				count = 1
				done = false
				return
			}

			if reply.VoteGranted {
				 // fmt.Printf("%d receive vote from %d at %d term\n",rf.me,serve,args.Term)
				count++
			}else {
				return
			}
			if done || count <= len(rf.peers)/2 {
				return
			}
			done = true
			//fmt.Printf("%d become leader at %d term\n",rf.me,rf.currentTerm)
			rf.convertToLeader()
		}(i,args,reply)
	}
	rf.reSetTime()
	rf.mu.Unlock()
}
func (rf *Raft) convertToFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.state = follower
}
func (rf *Raft) convertToCandidate() {
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.state = candidate
}
func (rf *Raft) convertToLeader(){


	rf.state = leader
	for i := 0;i < len(rf.peers) ;i++{
		if i == rf.me{
			rf.matchIndex[i] = rf.getLastlogIndex()
			continue
		}
		rf.nextIndex[i] = rf.getLastlogIndex()+1
		rf.matchIndex[i] = 0
	}
	go rf.heartBeats()
	go rf.attemptCommit()
}

func (rf *Raft) attemptCommit() {
	for {
		rf.mu.Lock()
		if rf.killed(){
			rf.mu.Unlock()
			return
		}
		if rf.state != leader {
			rf.mu.Unlock()
			return
		}
		var N int
		num := len(rf.peers)
		arr := make([]int,num)
		index := (num+1)/2
		for i:= 0;i<num;i++{
			arr[i] = rf.matchIndex[i]
		}
		//  sort.Ints(arr)
		for i:= 1;i<num;i++{
			for j := i;j >= 1;j--{
				if arr[j-1] <= arr[j]{
					break
				}
				temp := arr[j-1]
				arr[j-1] = arr[j]
				arr[j] = temp
			}
		}
		N = arr[index-1]
		//for i:=0;i<num;i++{
		//	fmt.Printf("%d ",arr[i])
		//}
		//fmt.Printf("\n")
		if  rf.commitIndex < N&&rf.getLogEntry(N).Term == rf.currentTerm {
			//fmt.Printf("%d commitIndex from %d to %d\n",rf.me,rf.commitIndex,N)
			rf.commitIndex = N
		}
		rf.mu.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}
func (rf *Raft)Apply(command interface{},index int)  {
	rf.applyCh <- ApplyMsg{Command: command,CommandValid: true,CommandIndex: index}
}
func (rf *Raft)attemptApply (){
	for {

		if rf.killed(){
			return
		}
		//var isleader State
		var applyLog []LogEntry
		k := 0
		rf.mu.Lock()//减小锁的粒度
		if rf.lastApplied < rf.snapShotIndex{
			rf.lastApplied = rf.snapShotIndex
		}
		cur_commitIndex := rf.commitIndex
		applyLog = make([]LogEntry,cur_commitIndex-rf.lastApplied)
		for i:=rf.lastApplied+1;i<=cur_commitIndex;i++{
		//	fmt.Printf("i is %d\n",i)
			applyLog[k] = rf.getLogEntry(i)
			k++
		}
		//isleader = rf.state
		rf.mu.Unlock()
		k = 0
		for rf.lastApplied < cur_commitIndex{
			rf.lastApplied++
			command :=  applyLog[k].Command
			k++
			index :=rf.lastApplied
			//if isleader==leader{
				//fmt.Printf("%d will take %v apply to serve commandIndex is %d\n",rf.me,command,index)
			//}
			rf.applyCh <- ApplyMsg{Command: command,CommandValid: true,CommandIndex: index}
		}
			//rf.applyCh <- ApplyMsg{Command: command,CommandValid: true,CommandIndex: index}

		//rf.mu.Unlock()
		time.Sleep(15 * time.Millisecond)
	}
}
func (rf *Raft) Me() int{
	return rf.me
}
//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int,len(rf.peers))
	rf.matchIndex = make([]int,len(rf.peers))

	rf.snapShotIndex=0
	rf.snapShotTerm=-1
	rf.applyCh = applyCh
	//rf.logs = make([]LogEntry,1)
	rf.logs = make([]LogEntry,0)

	//rf.flag = time.Now()
	// Your initialization code here (2A, 2B, 2C).
	rf.convertToFollower(0)
	rf.reSetTime()
	rf.termToindex = make(map[int]int)
	fmt.Printf("%d initialized\n",rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.commitIndex = rf.snapShotIndex
	rf.lastApplied = rf.snapShotIndex
	lens := rf.logLen()
	for i := 0;i<lens;i++{
		if rf.termToindex[rf.logs[i].Term] == 0{
			rf.termToindex[rf.logs[i].Term] = rf.logs[i].Index
		}
	}
	fmt.Printf("%d State is %v term is %d\n",rf.me,rf.state,rf.currentTerm)
	go rf.ticker()
	go rf.attemptApply()
	rf.mu.Unlock()

	return rf
}