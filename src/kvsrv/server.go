package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	db map[string]string

	ops map[int64]string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.

	key := args.Key

	// lastValue, duplicates := kv.check(args.LastSuccess, args.ID)
	// if duplicates {
	// 	reply.Value = lastValue
	// 	return
	// }

	kv.mu.Lock()
	defer kv.mu.Unlock()
	value := kv.db[key]
	reply.Value = value

	//kv.ops[args.ID] = reply.Value

}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key

	// lastValue, duplicates := kv.check(args.LastSuccess, args.ID)
	// if duplicates {
	// 	reply.Value = lastValue
	// 	return
	// }

	kv.mu.Lock()
	defer kv.mu.Unlock()
	value := args.Value
	kv.db[key] = value

	//kv.ops[args.ID] = reply.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	key := args.Key

	lastValue, duplicates := kv.check(args.LastSuccess, args.ID)
	if duplicates {
		reply.Value = lastValue
		return
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	oldValue := kv.db[key]
	reply.Value = oldValue
	kv.db[key] = oldValue + args.Value

	kv.ops[args.ID] = reply.Value
}

func (kv *KVServer) check(lastSuccess int64, id int64) (string, bool) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, ok := kv.ops[id]
	//fmt.Println("map length:", len(kv.ops))
	if ok == false {
		// no duplicates
		delete(kv.ops, lastSuccess)
		return "", false
	} else {
		return value, true
	}
}

// func (kv *KVServer) update(id int64, ans string) {
// 	kv.mu.Lock()
// 	defer kv.mu.Unlock()

// }

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.db = make(map[string]string)
	kv.ops = make(map[int64]string)
	return kv
}
