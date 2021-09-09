package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	PUT = "Put"
	APPEND = "Append"
	GET = "Get"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkId int64
	Seq     int
	OpType  string
	Key     string
	Value   string
}
type ServerReply struct {
	ClerkId int64
	Seq 	int
	Value string
	NoKey bool
}
type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db         map[string]string
	//ckSeq      map[int64]int
	waitlistch map[int][]chan ServerReply
	log_cout   int
	logIndex int
	ckseqFlag  map[int64]map[int]bool
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//fmt.Printf("server %d GET\n",kv.me)
	op := Op{OpType: GET,Key: args.Key,Value: "",ClerkId: args.ClerkId,Seq: args.Seq}
	index,term,isleader := kv.rf.Start(op)
	if !isleader{
	//	fmt.Printf("server %d GET notleader\n",kv.me)
		reply.Err=ErrWrongLeader
		return
	}
	kv.mu.Lock()
	ch := make(chan ServerReply)
	kv.waitlistch[index]=append(kv.waitlistch[index],ch)
	kv.mu.Unlock()
	//fmt.Printf("kv startRPC:%d server GetRPC of clerk %d seq %d Key %s: wait reply ,waitindex is %d\n",kv.me,args.ClerkId,args.Seq,args.Key,index)
	select {
	case serverReply:=<-ch:
		if  serverReply.ClerkId != args.ClerkId || serverReply.Seq!=args.Seq {
			reply.Err = ErrWrongLeader
			return
		}else {
			reply.Value=serverReply.Value
			reply.LeaderTerm=term
			reply.Err=OK
			reply.LeaderId = kv.me
			if serverReply.NoKey{
				reply.Err=ErrNoKey
			}
			return
		}
	case <-time.After(time.Second*1):
			reply.Err = ErrWrongLeader
	//	fmt.Printf("clerk %d seq %d send GETRPC to %d server timeout! KEY is %s\n",args.ClerkId,args.Seq,kv.me,args.Key)
		go func() {
			<-ch
		}()
		    return
	}

	//close(ch)
	//fmt.Printf("kv finishRPC:%d server GetRPC of clerk %d seq %d Key %s waitindex is %d error is %s Value is %s leaderterm is %d\n",kv.me,args.ClerkId,args.Seq,args.Key,index,reply.Err,reply.Value,term)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//fmt.Printf("servrer %d PUTAPPEND \n",kv.me)
	kv.mu.Lock()
	if flag,ok:=kv.ckseqFlag[args.ClerkId][args.Seq];ok&&flag{
			reply.Err=OK
			reply.LeaderTerm=-1
			//fmt.Printf("servrer %d PUTAPPEND ckSeq has been finished\n",kv.me)
			kv.mu.Unlock()
			return
	}
	kv.mu.Unlock()
	//kv.mu.Lock()
	//if kv.ckSeq[args.ClerkId] >= args.Seq{
	//	reply.Err=OK
	//	reply.LeaderTerm=-1
	//	fmt.Printf("servrer %d PUTAPPEND ckSeq is %d >= Seq %d\n",kv.me,kv.ckSeq[args.ClerkId],args.Seq)
	//	kv.mu.Unlock()
	//	return
	//}
	//if kv.ckSeq[args.ClerkId] != args.LastWriteSeq{//test case guaranteeing write operate is serial
	//fmt.Printf("servrer %d PUTAPPEND ckSeq is %d != lastSeq %d " +
	//		"and current seq is %d\n",kv.me,kv.ckSeq[args.ClerkId],args.LastWriteSeq,args.Seq)
	//	reply.Err=ErrWrongLeader
	//	kv.mu.Unlock()
	//	return
	//}
	//kv.mu.Unlock()
	op := Op{OpType: args.Op,Key: args.Key,Value: args.Value,ClerkId: args.ClerkId,Seq: args.Seq}
	index,term,isleader := kv.rf.Start(op)
	if !isleader{
		reply.Err=ErrWrongLeader
		//fmt.Printf("server %d PUTAPPEND notleader\n",kv.me)
		return
	}
	kv.mu.Lock()
	ch := make(chan ServerReply)
	kv.waitlistch[index]=append(kv.waitlistch[index],ch)
	kv.mu.Unlock()
	//fmt.Printf("kv startRPC:%d server %sRPC of clerk %d seq %d Key %s Value %s" +
	//	": wait reply ,waitindex is %d\n",kv.me,args.Op,args.ClerkId,args.Seq,args.Key,args.Value,index)
	select {
	case serverReply:=<-ch:
		if serverReply.ClerkId != args.ClerkId || serverReply.Seq!=args.Seq{
			reply.Err = ErrWrongLeader
		}else {
			reply.LeaderId = kv.me
			reply.LeaderTerm=term
			reply.Err=OK
		//	fmt.Printf("kv finishRPC:%d server %sRPC of clerk %d seq %d Key %s waitindex is %d " +
		//		"error is %s  leaderterm is %d\n",kv.me,args.Op,args.ClerkId,args.Seq,args.Key,index,reply.Err,term)
		}
	case <-time.After(time.Second*1):
		reply.Err = ErrWrongLeader
	//	fmt.Printf("clerk %d seq %d send %sRPC to %d server timeout! \n",args.ClerkId,args.Seq,args.Op,kv.me)
		go func() {
			<-ch
		}()
	}

	//close(ch)

}
func (kv *KVServer) CheckSnapShot() {
	for{
		if kv.killed(){
			return
		}
		time.Sleep(50*time.Millisecond)
		if kv.maxraftstate < 0{
			return
		}
		kv.mu.Lock()
		if kv.rf.GetRaftSize() < kv.maxraftstate{
			kv.mu.Unlock()
			continue
		}
		w := new(bytes.Buffer)
		e := labgob.NewEncoder(w)
		e.Encode(kv.db)
		e.Encode(kv.ckseqFlag)
		e.Encode(kv.logIndex)
		snapShot := w.Bytes()
		index := kv.logIndex
		kv.rf.Snapshot(index,snapShot)
		kv.mu.Unlock()
	}
}
func (kv *KVServer) serveApply(){
		for {
			if kv.killed(){
				return
			}
			//fmt.Printf("serverApply\n")
			applyMsg := <-kv.applyCh
			if applyMsg.CommandValid==false{
				fmt.Printf("serverApplyMsg commandvalid is false\n")
				if applyMsg.SnapshotValid == true{
					kv.mu.Lock()
					fmt.Printf("serverApplyMsg snapshotvalid is true\n")
					if kv.rf.CondInstallSnapshot(applyMsg.SnapshotTerm,applyMsg.SnapshotIndex,applyMsg.Snapshot) == true{
						r := bytes.NewBuffer(applyMsg.Snapshot)
						d := labgob.NewDecoder(r)
						var db map[string]string
						var check  map[int64]map[int]bool
						var preLogIndex int
						if d.Decode(&db) !=nil || d.Decode(&check)!= nil || d.Decode(&preLogIndex)!=nil{
							fmt.Printf("kv:snapshot error!\n")
						}else{
							kv.db = db
							kv.ckseqFlag = check
							kv.logIndex = preLogIndex
						}
					}
					kv.mu.Unlock()
				}
				continue
			}
			kv.mu.Lock()
			if applyMsg.CommandIndex < kv.logIndex{
				kv.mu.Unlock()
				continue
			}
			kv.log_cout++
			com := applyMsg.Command.(Op)
			kv.logIndex = applyMsg.CommandIndex
			nokey := false
			serverReply := ServerReply{ClerkId: com.ClerkId,Seq: com.Seq,Value: ""}
			waitlist := kv.waitlistch[applyMsg.CommandIndex]
			n := len(waitlist)

			flag := false
			if com.OpType == GET{
				value,ok := kv.db[com.Key]
				if ok{
					serverReply.Value=value
				}else {
					nokey = true
				}
				if n>0||flag{
					fmt.Printf("kv ApplyCom:%d server apply GET Key %s of clerk %d seq %d in %d index" +
						" and Value is %s lastIndex is %d\n",kv.me,com.Key,com.ClerkId,com.Seq,applyMsg.CommandIndex,value,kv.log_cout)
				}

			}else if pdp,okp:=kv.ckseqFlag[com.ClerkId][com.Seq]; com.OpType == PUT&&(!okp || !pdp) {
				kv.ckseqFlag[com.ClerkId] = make(map[int]bool)
				kv.ckseqFlag[com.ClerkId][com.Seq]=true
				//kv.ckSeq[com.ClerkId] = com.Seq
				kv.db[com.Key]=com.Value

				if n>0||flag{
					fmt.Printf("kv ApplyCom:%d server apply PUT Key %s of clerk %d seq %d in %d index" +
						" and Value is %s lastIndex is %d\n",kv.me,com.Key,com.ClerkId,com.Seq,applyMsg.CommandIndex,com.Value,kv.log_cout)
				}

			}else if pda,oka:=kv.ckseqFlag[com.ClerkId][com.Seq]; com.OpType == APPEND&&(!oka || !pda){
				var value string
				oldValue ,ok := kv.db[com.Key]
				kv.ckseqFlag[com.ClerkId] = make(map[int]bool)
				kv.ckseqFlag[com.ClerkId][com.Seq]=true
				//kv.ckSeq[com.ClerkId] = com.Seq
				if ok {
					var buffer bytes.Buffer
					buffer.WriteString(oldValue)
					buffer.WriteString(com.Value)
					newValue := buffer.String()
					kv.db[com.Key] = newValue
					value = newValue
				}else {
					kv.db[com.Key] = com.Value
					value = com.Value
				}
				if n > 0||flag{
					fmt.Printf("kv ApplyCom:%d server apply APPEND Key %s of clerk %d seq %d in %d index" +
						" and Value is %s lastIndex is %d\n",kv.me,com.Key,com.ClerkId,com.Seq,applyMsg.CommandIndex,value,kv.log_cout)
				}
			}
			kv.mu.Unlock()
			serverReply.NoKey = nokey
			for i:=0;i<n;i++{
				waitlist[i]<-serverReply
				close(waitlist[i])
			}
			if n>0{
				//fmt.Printf("kv ApplyCom:%d server finish replying in %d index"+
				//	" len is %d\n",kv.me,applyMsg.CommandIndex,n)
			}

		}
}
//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *KVServer) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var preDB map[string]string
	var preCheck map[int64]map[int]bool
	var preLogIndex int
	if d.Decode(&preDB) !=nil || d.Decode(&preCheck)!= nil || d.Decode(&preLogIndex)!=nil{
		fmt.Printf("KV:readPersist error!\n")
	}else {
		kv.db = preDB
		kv.ckseqFlag = preCheck
		kv.logIndex = preLogIndex
	}
}
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.db = make(map[string]string)
	kv.waitlistch = make(map[int][]chan ServerReply)
	kv.ckseqFlag = make(map[int64]map[int]bool)
	kv.log_cout=0
	// You may need initialization code here.
//	kv.ckSeq = make(map[int64]int)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.readPersist(persister.ReadSnapshot())
	// You may need initialization code here.
	go kv.serveApply()
	go kv.CheckSnapShot()
	return kv
}
