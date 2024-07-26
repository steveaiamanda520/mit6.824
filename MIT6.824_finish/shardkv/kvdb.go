package shardkv

type KvDataBase struct {
	KvData map[string]string
}

func (kv *KvDataBase) Init() {
	kv.KvData = make(map[string]string)
}

func (kv *KvDataBase) Clone() KvDataBase {
	db := KvDataBase{}
	db.KvData = make(map[string]string)
	for key, value := range kv.KvData {
		db.KvData[key] = value
	}
	return db
}
func (kv *KvDataBase) Get(key string) (value string, ok bool) {
	if value, ok := kv.KvData[key]; ok {
		return value, ok
	}
	return "", ok
}

func (kv *KvDataBase) Put(key string, value string) (newValue string) {
	kv.KvData[key] = value
	return value
}

func (kv *KvDataBase) Append(key string, arg string) (newValue string) {
	if value, ok := kv.KvData[key]; ok {
		newValue := value + arg
		kv.KvData[key] = newValue
		return newValue
	}
	kv.KvData[key] = arg
	return arg
}

func (kv *KvDataBase) Clear() {
	kv.KvData = make(map[string]string)
}
