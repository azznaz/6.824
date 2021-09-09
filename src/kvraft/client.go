package kvraft

import (
	"../labrpc"
	"fmt"
	"sync"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu           sync.Mutex
	leaderId int
	leaderTerm int
	clerkId  int64
	seq  	 int
	lastWriteSeq int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clerkId = nrand()
	ck.leaderId = 0
	ck.leaderTerm = 0
	ck.lastWriteSeq = 0
	ck.seq = 0
	return ck
}
func (ck *Clerk) sendGetRPC(server int, args *GetArgs, reply *GetReply ) bool {
	ok := ck.servers[server].Call("KVServer.Get",args, reply)
	return ok
}
func (ck *Clerk) sendPutAppendRPC(server int, args *PutAppendArgs, reply *PutAppendReply ) bool {
	ok := ck.servers[server].Call("KVServer.PutAppend",args, reply)
	return ok
}
//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.

	var nowLeader int
	var seq int
	ck.mu.Lock()
	nowLeader = ck.leaderId
	ck.seq++
	seq = ck.seq
	ck.mu.Unlock()
	res :=""

	arr := make([]int64,11)
	arr[0]=1
	for i:=1;i<11;i++{
		arr[i]=arr[i-1]*2
		if arr[i]>=32{
			arr[i]=32
		}
	}
	k:=0
	for {
		time.Sleep(time.Millisecond*time.Duration(20+arr[k]))
		if k>=10{
			k=10
		}else {
			k++
		}
		args := GetArgs{Key: key,ClerkId: ck.clerkId,Seq: seq}
		reply := GetReply{Value: "",Err: "",LeaderId: 0,LeaderTerm: 0}
		ok := ck.sendGetRPC(nowLeader,&args,&reply)
		if !ok {
			nowLeader = (nowLeader+1)%len(ck.servers)
			continue
		}
		if reply.Err == ErrWrongLeader {
			nowLeader = (nowLeader+1)%len(ck.servers)
			continue
		}
		fmt.Printf("clerk %d send GetRPC to %d server and Seq is %d Key is %s " +
			"reply leader is %d\n",args.ClerkId,nowLeader,args.Seq,args.Key,reply.LeaderId)
		ck.mu.Lock()
		if ck.leaderTerm <= reply.LeaderTerm{
			ck.leaderId = nowLeader
			ck.leaderTerm = reply.LeaderTerm
		}
		ck.mu.Unlock()
		res = reply.Value
		fmt.Printf("finish GetRPC clerk: %d Seq: %d Value :%s leaderId is %d term is %d " +
			"reply leader is %d\n",args.ClerkId,args.Seq,res,nowLeader,reply.LeaderTerm,reply.LeaderId)
		break
	}
	return res
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.

	var nowLeader int
	var seq int
	var lastWriteSeq int
	ck.mu.Lock()
	ck.seq++
	nowLeader = ck.leaderId
	seq=ck.seq
	lastWriteSeq=ck.lastWriteSeq
	ck.lastWriteSeq=seq
	ck.mu.Unlock()
	arr := make([]int64,11)
	arr[0]=1

	for i:=1;i<11;i++{
		arr[i]=arr[i-1]*2
		if arr[i]>=256{
			arr[i]=256
		}
	}
	k:=0
	for {
		time.Sleep(time.Millisecond*time.Duration(20+arr[k]))
		if k>=10{
			k=10
		}else {
			k++
		}
		args := PutAppendArgs{Key: key,ClerkId: ck.clerkId,Value: value,Op: op,LastWriteSeq: lastWriteSeq,Seq: seq}
		reply := PutAppendReply{Err: "",LeaderId: 0,LeaderTerm: 0}
		ok := ck.sendPutAppendRPC(nowLeader,&args,&reply)
		if !ok {
			nowLeader = (nowLeader+1)%len(ck.servers)
			continue
		}
		if reply.Err == ErrWrongLeader{
			nowLeader = (nowLeader+1)%len(ck.servers)
			continue
		}
		fmt.Printf("clerk %d send %sRPC to %d server and Seq is %d Key is %s Value is %s" +
			" reply leader is %d\n",args.ClerkId,args.Op,nowLeader,args.Seq,args.Key,args.Value,reply.LeaderId)
		ck.mu.Lock()
		if ck.leaderTerm <= reply.LeaderTerm{
			ck.leaderId = nowLeader
			ck.leaderTerm = reply.LeaderTerm
		}
		ck.mu.Unlock()
		fmt.Printf("finish %sPRC clerk: %d Seq: %d leaderId is %d term is %d reply leader is %d\n",args.Op,args.ClerkId,args.Seq,nowLeader,reply.LeaderTerm,reply.LeaderId)
		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
