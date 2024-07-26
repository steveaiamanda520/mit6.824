package shardkv

import (
	"6.824/labrpc"
	"6.824/shardctrler"
	"bytes"
	"encoding/gob"
	"sync/atomic"
	"time"
)
import "6.824/raft"
import "sync"
import "6.824/labgob"

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	OpType OpTypeT
	Data   interface{}
}
type DataArgs struct {
	Key       string
	Value     string
	ClientId  int64
	CommandId int
}
type ApplyResult struct {
	ErrMsg Err
	Value  string
	Term   int
}
type CommandContext struct {
	CommandId int
	Reply     ApplyResult
}
type ShardData struct {
	ShardNum int        // shard的编号
	KvDB     KvDataBase // 数据库
	// ClientReply map[int64]CommandContext // 该shard中，缓存回复client的内容。
}

func (srcData *ShardData) clone() ShardData {
	copyData := ShardData{}
	// copyData.ClientReply = make(map[int64]CommandContext)
	copyData.ShardNum = srcData.ShardNum
	copyData.KvDB = srcData.KvDB.Clone()
	//for clinetId, msg := range srcData.ClientReply {
	//	copyData.ClientReply[clinetId] = msg
	//}
	return copyData
}

func (data *ShardData) clear() {
	// data.ClientReply = make(map[int64]CommandContext)
	data.KvDB.Clear()

}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	makeEnd      func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big
	// Your definitions here.
	shardData        [shardctrler.NShards]ShardData
	clientReply      map[int64]CommandContext
	replyChMap       map[int]chan ApplyResult // log的index对应的返回信息。
	gcReplyMap       map[int]GCReply
	lastAppliedIndex int // kv数据库中最后应用的index.
	mck              *shardctrler.Clerk
	dead             int32
	cfg              shardctrler.Config
	prevCfg          shardctrler.Config // 之前的cfg
	shardStatus      [shardctrler.NShards]ShardStatus
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.cfg.Shards[shard] != kv.gid {
		// 目前不负责该shard
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.shardStatus[shard] != Serving {
		// 目前负责该shard，但是还没有准备好。（pulling状态）
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if clientReply, ok := kv.clientReply[args.ClientId]; ok {
		if args.CommandId == clientReply.CommandId {
			reply.Value = clientReply.Reply.Value
			reply.Err = clientReply.Reply.ErrMsg
			kv.mu.Unlock()
			return
		} else if args.CommandId < clientReply.CommandId {
			reply.Err = ErrOutDate // 已经没有了之前执行的记录。
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	op := Op{
		OpType: GetOperation,
		Data: DataArgs{
			Key:       args.Key,
			Value:     "",
			CommandId: args.CommandId,
			ClientId:  args.ClientId},
	}

	index, term, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	DPrintf("Leader[%d.%d] Receive Op\t:%v\tIndex[%d]", kv.gid, kv.me, op, index)
	replyCh := make(chan ApplyResult, 1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()
	select {
	case replyMsg := <-replyCh:
		{
			if term != replyMsg.Term {
				// 已经进入之后的term，leader改变（当前server可能仍然是leader，但是已经是几个term之后了）
				reply.Err = ErrWrongLeader
				return
			} else {
				reply.Value = replyMsg.Value
				reply.Err = replyMsg.ErrMsg
			}
		}
	case <-time.After(KVTimeOut):
		{
			reply.Err = ErrWrongLeader
			return
		}
	}
	go kv.CloseIndexCh(index)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	// DPrintf("Server [%d.%d] Receive Request\t:%v", kv.gid, kv.me, args)
	kv.mu.Lock()
	shard := key2shard(args.Key)
	if kv.cfg.Shards[shard] != kv.gid {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	if kv.shardStatus[shard] != Serving {
		// 目前负责该shard，但是还没有准备好。（pulling状态）
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if clientReply, ok := kv.clientReply[args.ClientId]; ok {
		if args.CommandId == clientReply.CommandId {
			DPrintf("Server [%d] Receive Same Command [%d]", kv.me, args.CommandId)
			reply.Err = clientReply.Reply.ErrMsg
			kv.mu.Unlock()
			return
		} else if args.CommandId < clientReply.CommandId {
			DPrintf("Server  [%d] Receive OutDate Command [%d]", kv.me, args.CommandId)
			reply.Err = ErrOutDate // 已经没有了之前执行的记录。
			kv.mu.Unlock()
			return
		}
	}
	kv.mu.Unlock()
	op := Op{
		OpType: OpTypeT(args.Op),
		Data: DataArgs{
			Key:       args.Key,
			Value:     args.Value,
			CommandId: args.CommandId,
			ClientId:  args.ClientId},
	}

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		// DPrintf("Server[%d.%d] Isn't Leader", kv.gid, kv.me)
		return
	}
	DPrintf("Leader[%d.%d] Receive Op\t:%v\tIndex[%d]", kv.gid, kv.me, op, index)
	replyCh := make(chan ApplyResult, 1)
	kv.mu.Lock()
	kv.replyChMap[index] = replyCh
	kv.mu.Unlock()
	select {
	case replyMsg := <-replyCh:
		{
			if term != replyMsg.Term {
				// 已经进入之后的term，leader改变（当前server可能仍然是leader，但是已经是几个term之后了）
				reply.Err = ErrWrongLeader
			} else {
				reply.Err = replyMsg.ErrMsg
			}
		}
	case <-time.After(KVTimeOut):
		{
			reply.Err = ErrWrongLeader
			return
		}
	}
	go kv.CloseIndexCh(index)
}
func (kv *ShardKV) CloseIndexCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	indexCh, ok := kv.replyChMap[index]
	if !ok {
		return
	}
	close(indexCh)
	delete(kv.replyChMap, index)
	return
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	atomic.StoreInt32(&kv.dead, 1)
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardctrler.
//
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
//
// makeEnd(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use ctrlers[]
// and makeEnd() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(shardctrler.Config{})
	labgob.Register(DataArgs{})
	labgob.Register(PullDataReply{})
	labgob.Register(PullDataArgs{})
	labgob.Register(GCArgs{})
	labgob.Register(GCReply{})
	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.makeEnd = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.

	// Use something like this to talk to the shardctrler:
	kv.mck = shardctrler.MakeClerk(kv.ctrlers)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.replyChMap = make(map[int]chan ApplyResult)
	kv.gcReplyMap = make(map[int]GCReply)
	kv.clientReply = make(map[int64]CommandContext)
	for i := 0; i < 10; i++ {
		kv.shardData[i] = ShardData{}
		kv.shardData[i].ShardNum = i
		kv.shardData[i].KvDB.Init()
		// kv.shardData[i].ClientReply = make(map[int64]CommandContext)
		kv.shardStatus[i] = Invalid
	}
	kv.ReadSnapshot(persister.ReadSnapshot())
	kv.lastAppliedIndex = int(kv.rf.GetLastIncludeIndex())
	go kv.handleApplyMsg()
	go kv.daemon(kv.fetchNewConfig, 50*time.Millisecond)
	go kv.daemon(kv.pullData, 100*time.Millisecond)
	go kv.daemon(kv.garbageCollector, 100*time.Millisecond)
	go kv.daemon(kv.testEmptyLog, 200*time.Millisecond)
	go kv.makeSnapshot()
	return kv
}
func (kv *ShardKV) handleApplyMsg() {
	for !kv.killed() {
		applyMsg := <-kv.applyCh

		if applyMsg.CommandValid {
			kv.applyCommand(applyMsg)
			if kv.rf.GetRaftStateSize() > kv.maxraftstate && kv.maxraftstate != -1 {
				kv.doSnapshot()
			}
			continue
		}
		if applyMsg.SnapshotValid {
			kv.applySnapshot(applyMsg)
			continue
		}
		DPrintf("Error Apply Msg: [%v]", applyMsg)
	}
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}
func (kv *ShardKV) applyCommand(msg raft.ApplyMsg) {
	op := msg.Command.(Op)
	if msg.CommandIndex <= kv.lastAppliedIndex {
		// 按理说不应该发生这种情况
		return
	}
	switch op.OpType {
	case GetOperation:
		kv.applyDBModify(msg)
	case PutOperation:
		kv.applyDBModify(msg)
	case AppendOperation:
		kv.applyDBModify(msg)
	case NewConfig:
		kv.applyNewConfig(msg.CommandIndex, op.Data.(shardctrler.Config))
	case PullNewData:
		kv.applyShard(msg.CommandIndex, op.Data.(PullDataReply))
	case ConfirmGC:
		kv.applyConfirmGC(msg)
	case ConfirmPull:
		kv.applyConfirmPull(msg)
	case EmptyLog:
		kv.applyEmptyLog(msg)
	default:
		DPrintf("ERROR OP TYPE:%s", op.OpType)
	}
}
func (kv *ShardKV) applyDBModify(msg raft.ApplyMsg) {
	var applyResult ApplyResult
	op := msg.Command.(Op)
	applyResult.Term = msg.CommandTerm
	args := op.Data.(DataArgs)
	shardNum := key2shard(args.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	commandContext, ok := kv.clientReply[args.ClientId]
	if ok && commandContext.CommandId >= args.CommandId {
		// 该指令已经被应用过。
		// applyResult = commandContext.Reply
		return
	}
	if !kv.canServe(shardNum) {
		ch, ok := kv.replyChMap[msg.CommandIndex]
		applyResult.ErrMsg = ErrWrongLeader
		if ok {
			ch <- applyResult
		}
		return
	}
	switch op.OpType {
	case GetOperation:
		{
			value, ok := kv.shardData[shardNum].KvDB.Get(args.Key)
			// DPrintf("Server[%d.%d] Get K[%v] V[%v] From Shard[%d]", kv.gid, kv.me, args.Key, value, shardNum)
			if ok {
				applyResult.Value = value
				applyResult.ErrMsg = OK
			} else {
				applyResult.Value = ""
				applyResult.ErrMsg = ErrNoKey
			}
		}
	case PutOperation:
		{
			value := kv.shardData[shardNum].KvDB.Put(args.Key, args.Value)
			// DPrintf("Server[%d.%d] Put K[%v] V[%v] To Shard[%d]", kv.gid, kv.me, args.Key, args.Value, shardNum)
			applyResult.Value = value
			applyResult.ErrMsg = OK
		}
	case AppendOperation:
		{
			value := kv.shardData[shardNum].KvDB.Append(args.Key, args.Value)
			DPrintf("Server[%d.%d] Append K[%v] V[%v] To Shard[%d] ShardStatus[%v]", kv.gid, kv.me, args.Key, args.Value, shardNum, kv.shardStatus)
			applyResult.Value = value
			applyResult.ErrMsg = OK
		}
	default:
		DPrintf("Error Op Type %s", op.OpType)
	}
	kv.lastAppliedIndex = msg.CommandIndex
	ch, ok := kv.replyChMap[msg.CommandIndex]
	if ok {
		ch <- applyResult
	}
	kv.clientReply[args.ClientId] = CommandContext{
		CommandId: args.CommandId,
		Reply:     applyResult,
	}
}
func (kv *ShardKV) applyNewConfig(idx int, config shardctrler.Config) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if config.Num <= kv.cfg.Num {
		DPrintf("[%d.%d]Try Apply Outdated Config[%d]", kv.gid, kv.me, config.Num)
		return
	}
	for shard := 0; shard < shardctrler.NShards; shard++ {
		if kv.shardStatus[shard] != Serving && kv.shardStatus[shard] != Invalid {
			// 还有迁移没有完成
			return
		}
	}
	kv.updateShardsStatus(config.Shards)
	DPrintf("Server [%d.%d] Convert [%d]To new Config :[%v] Status:[%v]", kv.gid, kv.me, kv.cfg.Num, config, kv.shardStatus)
	kv.prevCfg = kv.cfg
	kv.cfg = config
	kv.lastAppliedIndex = idx
	go kv.pullData()
}
func (kv *ShardKV) fetchNewConfig() {
	kv.mu.RLock()
	newCfgNum := kv.cfg.Num + 1
	kv.mu.RUnlock()

	newCfg := kv.mck.Query(newCfgNum) // 保证config递增
	if newCfg.Num == newCfgNum {
		// 配置发生了变化，检查自己不负责哪些shard了。
		op := Op{
			OpType: NewConfig,
			Data:   newCfg,
		}
		// kv.mu.Lock()
		if _, isLeader := kv.rf.GetState(); isLeader {
			kv.rf.Start(op)
		}
		// kv.mu.Unlock()
		time.Sleep(100 * time.Millisecond)
		return
	}
}

// 守护进程，负责获取新config的分区
func (kv *ShardKV) pullData() {
	wg := sync.WaitGroup{}
	kv.mu.RLock()
	if _, isLeader := kv.rf.GetState(); !isLeader {
		kv.mu.RUnlock()
		return
	}
	pullingShards := kv.getTargetGidAndShardsByStatus(Pulling)
	for gid, shards := range pullingShards {
		wg.Add(1)
		go func(config shardctrler.Config, gid int, shards []int, configNum int) {
			defer wg.Done()
			servers := config.Groups[gid]
			args := PullDataArgs{
				PulledShard: shards,
				ConfigNum:   configNum,
				Gid:         kv.gid,
			}
			for _, server := range servers {
				var reply PullDataReply
				if ok := kv.makeEnd(server).Call("ShardKV.PullData", &args, &reply); ok && reply.ErrMsg == OK {
					kv.rf.Start(Op{
						OpType: PullNewData,
						Data:   reply,
					})
				}
			}
		}(kv.prevCfg, gid, shards, kv.cfg.Num)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

type PullDataArgs struct {
	PulledShard []int // 需要拉取的shards
	ConfigNum   int   // 请求group的版本号。
	Gid         int   // 请求group的id
}
type PullDataReply struct {
	Data        []ShardData
	ClientReply map[int64]CommandContext
	ConfigNum   int // 请求group的版本号。
	ErrMsg      Err
}

func (kv *ShardKV) PullData(args *PullDataArgs, reply *PullDataReply) {

	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.ErrMsg = ErrWrongLeader
		return
	}
	if args.ConfigNum > kv.cfg.Num {
		reply.ErrMsg = ErrNotReady
		return
	}
	//if args.ConfigNum < kv.cfg.Num {
	//	// 如果发生这种情况，会怎么样？同步数据给一个过去的Data。
	//	panic(ErrOutDate)
	//}
	for _, shard := range args.PulledShard {
		reply.Data = append(reply.Data, kv.shardData[shard].clone())
	}
	reply.ClientReply = make(map[int64]CommandContext)
	for key, value := range kv.clientReply {
		reply.ClientReply[key] = value
	}
	reply.ErrMsg = OK
	reply.ConfigNum = kv.cfg.Num
	return
}
func (kv *ShardKV) daemon(action func(), duration time.Duration) {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); isLeader {
			action()
		}
		time.Sleep(duration)
	}
}

/*
*
更新分片的状态。
*/
func (kv *ShardKV) updateShardsStatus(newShards [10]int) {
	oldShards := kv.cfg.Shards
	for shard, gid := range oldShards {
		if gid == kv.gid && newShards[shard] != kv.gid {
			// 本来该shard由本group负责， 但是现在应该迁移给别人
			if newShards[shard] != 0 {
				kv.shardStatus[shard] = WaitPull
			} else {
				kv.shardStatus[shard] = Invalid
			}
		}
		if newShards[shard] == kv.gid && gid != kv.gid {
			if gid != 0 {
				// 从别的shard迁移过来
				kv.shardStatus[shard] = Pulling
			} else {
				// 从无效分片过来，直接开始服务
				kv.shardStatus[shard] = Serving
			}
		}
	}
}

/*
*	根据目前指定的状态，指定之前的GID.
*
* 	比如目前的配置是：shard[5] = {1,1,1,1,3} ,kv.me = 101
* 	shardStatus[5] = {serving, pulling, pulling, pulling, invalid}
* 	可能第1，2个分片（从0开始）需要从Group102拉取，第3个分片应当从Group103拉取。
*	那么返回map:[{102,[1,2]},{103,[3]}]
 */
func (kv *ShardKV) getTargetGidAndShardsByStatus(targetStatus ShardStatus) map[int][]int {
	result := make(map[int][]int)
	for shardIndex, shardStatus := range kv.shardStatus {
		if shardStatus == targetStatus {
			prevGid := kv.prevCfg.Shards[shardIndex]
			if prevGid != 0 {
				// 找到之前的GID，并加入shard编号
				_, ok := result[prevGid]
				if !ok {
					result[prevGid] = make([]int, 0)
				}
				result[prevGid] = append(result[prevGid], shardIndex)
			}
		}
	}
	return result
}

func (kv *ShardKV) applyShard(idx int, reply PullDataReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if reply.ConfigNum == kv.cfg.Num {
		for _, shardData := range reply.Data {
			if kv.shardStatus[shardData.ShardNum] == Pulling {
				// 注意不要被重复的pull data覆盖了状态。
				DPrintf("[%d.%d] Apply Shard Data[%d.%d]:[%v]", kv.gid, kv.me, kv.cfg.Num, shardData.ShardNum, shardData.KvDB.KvData)
				kv.shardData[shardData.ShardNum] = shardData.clone()
				kv.shardStatus[shardData.ShardNum] = ServingButGC
			} else {
				DPrintf("[%d.%d] Avoid Covering Data", kv.gid, kv.me)
			}

		}
		for key, value := range reply.ClientReply {
			prevContent, ok := kv.clientReply[key]
			if !ok || prevContent.CommandId < value.CommandId {
				kv.clientReply[key] = value
			}
		}

	}
	kv.lastAppliedIndex = idx
	return
}

// 守护进程，负责通知需要garbage collect分区的服务器组。
func (kv *ShardKV) garbageCollector() {
	wg := sync.WaitGroup{}
	kv.mu.RLock()
	gcShards := kv.getTargetGidAndShardsByStatus(ServingButGC)
	for gid, shards := range gcShards {
		wg.Add(1)
		go func(config shardctrler.Config, gid int, shards []int, cfgNum int) {
			defer wg.Done()
			servers := config.Groups[gid]
			args := GCArgs{
				GCShard:   shards,
				ConfigNum: cfgNum,
				Gid:       kv.gid,
			}
			for _, server := range servers {
				var reply GCReply
				if ok := kv.makeEnd(server).Call("ShardKV.GCHandler", &args, &reply); ok && reply.ErrMsg == OK {
					kv.rf.Start(Op{
						OpType: ConfirmGC,
						Data:   args,
					})
					return
				}
			}
		}(kv.prevCfg, gid, shards, kv.cfg.Num)
	}
	kv.mu.RUnlock()
	wg.Wait()
}

type GCArgs struct {
	GCShard   []int // 需要gc的shards
	ConfigNum int   // 请求group的版本号。
	Gid       int   // 请求group的id
}
type GCReply struct {
	ConfigNum int // 请求group的版本号。
	ErrMsg    Err
}

func (kv *ShardKV) GCHandler(args *GCArgs, reply *GCReply) {
	kv.mu.Lock()
	if prevReply, ok := kv.gcReplyMap[args.Gid]; ok && (prevReply.ErrMsg == OK) && (prevReply.ConfigNum == args.ConfigNum) {
		reply.ErrMsg = OK
		reply.ConfigNum = args.ConfigNum
		kv.mu.Unlock()
		return
	}
	if _, isleader := kv.rf.GetState(); !isleader {
		reply.ErrMsg = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	if args.ConfigNum > kv.cfg.Num {
		reply.ErrMsg = ErrNotReady
		kv.mu.Unlock()
		return
	}
	if args.ConfigNum < kv.cfg.Num {
		// 不太可能发生这种情况，因为如果shard group没有完全同步，不会切换为下一个config
		// 可能出现在重启的情况中？
		reply.ErrMsg = ErrOutDate
		kv.mu.Unlock()
		return
	}
	index, currentTerm, isleader := kv.rf.Start(Op{
		OpType: ConfirmPull,
		Data:   *args,
	})
	if !isleader {
		reply.ErrMsg = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	applyCh := make(chan ApplyResult, 1)
	kv.replyChMap[index] = applyCh
	kv.mu.Unlock()

	select {
	case replyMsg := <-applyCh:
		{
			if currentTerm != replyMsg.Term {
				// 已经进入之后的term，leader改变（当前server可能仍然是leader，但是已经是几个term之后了）
				// 说明执行的结果不是同一个log的
				reply.ErrMsg = ErrWrongLeader
				return
			} else {
				reply.ConfigNum = args.ConfigNum
				reply.ErrMsg = OK
			}
		}
	case <-time.After(500 * time.Millisecond):
		{
			reply.ErrMsg = ErrWrongLeader
		}
	}
	go kv.CloseIndexCh(index)
}
func (kv *ShardKV) applyConfirmGC(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	op := msg.Command.(Op)
	args := op.Data.(GCArgs)
	if args.ConfigNum == args.ConfigNum {
		for _, shard := range args.GCShard {
			kv.shardStatus[shard] = Serving
		}
		DPrintf("[%d.%d] ConfirmGC Config: [%d], Status:[%v]", kv.gid, kv.me, args.ConfigNum, kv.shardStatus)
		kv.lastAppliedIndex = msg.CommandIndex
		return
	}
	DPrintf("Try To Apply OutDated Config [%d]", args.ConfigNum)
	return
}

/*
*
shard 从wait pull状态到invalid
*/
func (kv *ShardKV) applyConfirmPull(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	args := msg.Command.(Op).Data.(GCArgs)
	if kv.gcReplyMap[args.Gid].ConfigNum >= args.ConfigNum {
		DPrintf("Try Apply OutDated Confirm Pull")
		return
	}
	for _, shard := range args.GCShard {
		kv.shardStatus[shard] = Invalid
		kv.shardData[shard].clear()
	}
	result := ApplyResult{
		ErrMsg: OK,
		Value:  "",
		Term:   msg.CommandTerm,
	}
	if ch, ok := kv.replyChMap[msg.CommandIndex]; ok {
		ch <- result
	}
	kv.lastAppliedIndex = msg.CommandIndex
	kv.gcReplyMap[args.Gid] = GCReply{
		ConfigNum: args.ConfigNum,
		ErrMsg:    OK,
	}
	kv.lastAppliedIndex = msg.CommandIndex
	DPrintf("[%d.%d] Confirm Pull: [%d], Status:[%v]", kv.gid, kv.me, args.ConfigNum, kv.shardStatus)
}
func (kv *ShardKV) makeSnapshot() {
	for !kv.killed() {
		time.Sleep(time.Millisecond * 100)
		kv.doSnapshot()
	}
}

func (kv *ShardKV) doSnapshot() {
	kv.mu.Lock()
	sizeNow := kv.rf.GetRaftStateSize()
	// DPrintf("Server[%d] Raft Size Is [%d] LastIncludeIndex [%d]", kv.me, sizeNow, kv.rf.GetLastIncludeIndex())
	if sizeNow > kv.maxraftstate && kv.maxraftstate != -1 {
		// DPrintf("Server[%d.%d] Start to Make Snapshot", kv.gid, kv.me)
		kv.rf.Snapshot(kv.lastAppliedIndex, kv.GenSnapshot())
		// DPrintf("Server[%d.%d] Raft Size Is [%d] After Snapshot", kv.gid, kv.me, kv.rf.GetRaftStateSize())
	}
	kv.mu.Unlock()
}

func (kv *ShardKV) GenSnapshot() []byte {
	w := new(bytes.Buffer)
	encode := gob.NewEncoder(w)
	if encode.Encode(kv.cfg) != nil ||
		encode.Encode(kv.prevCfg) != nil ||
		encode.Encode(kv.shardStatus) != nil ||
		encode.Encode(kv.gcReplyMap) != nil ||
		encode.Encode(kv.clientReply) != nil {
		panic("Can't Generate Snapshot")
		return nil
	}
	for _, data := range kv.shardData {
		if encode.Encode(data.clone()) != nil {
			panic("Can't Generate Snapshot")
			return nil
		}
	}

	return w.Bytes()
}
func (kv *ShardKV) ReadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	decode := gob.NewDecoder(r)
	if decode.Decode(&kv.cfg) != nil ||
		decode.Decode(&kv.prevCfg) != nil ||
		decode.Decode(&kv.shardStatus) != nil ||
		decode.Decode(&kv.gcReplyMap) != nil ||
		decode.Decode(&kv.clientReply) != nil {
		panic("Can't Read Snapshot")
	}
	for i, _ := range kv.shardData {
		if decode.Decode(&kv.shardData[i]) != nil {
			panic("Can't Read Snapshot")
		}
	}
}

func (kv *ShardKV) applySnapshot(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if kv.rf.CondInstallSnapshot(msg.SnapshotTerm, msg.SnapshotIndex, msg.Snapshot) {
		kv.lastAppliedIndex = msg.SnapshotIndex
		kv.ReadSnapshot(msg.Snapshot)
	}
}
func (kv *ShardKV) canServe(shardNum int) bool {
	return kv.cfg.Shards[shardNum] == kv.gid && (kv.shardStatus[shardNum] == Serving || kv.shardStatus[shardNum] == ServingButGC)
}

// 定时添加空日志
func (kv *ShardKV) testEmptyLog() {
	kv.rf.Start(Op{
		OpType: EmptyLog,
		Data:   nil,
	})
}

func (kv *ShardKV) applyEmptyLog(msg raft.ApplyMsg) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.lastAppliedIndex = msg.CommandIndex
}
