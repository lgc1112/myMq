package consistenthash

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)
type ConsistenceHash struct {
	sync.RWMutex
	nodesMap        map[uint32]string // hash slot和虚拟node的映射关系
	nodesSlots      slots // 虚拟node所有hash slot组成的切片
	NumVirtualNodes int // 为每台机器在hash圆环上创建多少个虚拟Node
}

// 使用sort.Sort函数，传入的参数需要实现的接口
type slots []uint32

func (s slots) Len() int {
	return len(s)
}

func (s slots) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s slots) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func New(NumVirtualNodes int) *ConsistenceHash {
	m := &ConsistenceHash{
		NumVirtualNodes: NumVirtualNodes,
		nodesMap:  make(map[uint32]string),
	}
	return m
}

// 通过crc32函数计算散列值
func (h *ConsistenceHash) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// 集群中增加机器
func (h *ConsistenceHash) AddNode(addr string) {
	h.Lock()
	defer h.Unlock()
	// 根据定义的数量生成虚拟Node
	// addr加上不同的后缀计算散列值得到每个虚拟Node的hash slot
	// 同一个机器的所有hash slot最终都指向同一个ip/port
	for i := 0; i < h.NumVirtualNodes; i++ {
		slot := h.hash(fmt.Sprintf("%s%d", addr, i))
		h.nodesMap[slot] = addr
	}
	h.sortNodesSlots()
}

// 所有虚拟Node映射到的hash slot排序后保存到切片
func (h *ConsistenceHash) sortNodesSlots() {
	slots := h.nodesSlots[:]
	for slot := range h.nodesMap {
		slots = append(slots, slot)
	}
	sort.Sort(slots)
	h.nodesSlots = slots
}

// 从集群中摘除机器
func (h *ConsistenceHash) DeleteNode(addr string) {
	h.Lock()
	defer h.Unlock()
	// 删除所有的虚拟节点
	for i := 0; i < h.NumVirtualNodes; i++ {
		slot := h.hash(fmt.Sprintf("%s%d", addr, i))
		delete(h.nodesMap, slot)
	}
	h.sortNodesSlots()
}

// 查找一个key对应的读写Node
func (h *ConsistenceHash) SearchNode(key string) (string, error) {
	slot := h.hash(key)
	// 使用sort包的二分查找函数
	f := func(x int) bool {
		return h.nodesSlots[x] >= slot
	}
	index := sort.Search(len(h.nodesSlots), f)
	if index >= len(h.nodesSlots) {
		index = 0
	}
	if addr, ok := h.nodesMap[h.nodesSlots[index]]; ok {
		return addr, nil
	} else {
		return addr, errors.New("not found")
	}
}


//
//
//
//type UInt32Slice []uint32
//
//func (s UInt32Slice) Len() int {
//	return len(s)
//}
//
//func (s UInt32Slice) Less(i, j int) bool {
//	return s[i] < s[j]
//}
//
//func (s UInt32Slice) Swap(i, j int) {
//	s[i], s[j] = s[j], s[i]
//}
//
////type Hash func(data []byte) uint32
//
//type Map struct {
//	hash     func(data []byte) uint32
//	replicas int               // 复制因子
//	keys     UInt32Slice       // 已排序的节点哈希切片
//	hashMap  map[uint32]string // 节点哈希和KEY的map，键是哈希值，值是节点Key
//}
//
//func New(replicas int) *Map {
//	m := &Map{
//		replicas: replicas,
//		hashMap:  make(map[uint32]string),
//	}
//	// 默认使用CRC32算法
//	if m.hash == nil {
//		m.hash = crc32.ChecksumIEEE
//	}
//	return m
//}
//
//func (m *Map) IsEmpty() bool {
//	return len(m.keys) == 0
//}
//
//// Add 方法用来添加缓存节点，参数为节点key，比如使用IP
//func (m *Map) Add(keys ...string) {
//	for _, key := range keys {
//		// 结合复制因子计算所有虚拟节点的hash值，并存入m.keys中，同时在m.hashMap中保存哈希值和key的映射
//		for i := 0; i < m.replicas; i++ {
//			hash := m.hash([]byte(strconv.Itoa(i) + key))
//			m.keys = append(m.keys, hash)
//			m.hashMap[hash] = key
//		}
//	}
//	// 对所有虚拟节点的哈希值进行排序，方便之后进行二分查找
//	sort.Sort(m.keys)
//}
//
//// Get 方法根据给定的对象获取最靠近它的那个节点key
//func (m *Map) Get(key string) string {
//	if m.IsEmpty() {
//		return ""
//	}
//
//	hash := m.hash([]byte(key))
//
//	// 通过二分查找获取最优节点，第一个节点hash值大于对象hash值的就是最优节点
//	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })
//
//	// 如果查找结果大于节点哈希数组的最大索引，表示此时该对象哈希值位于最后一个节点之后，那么放入第一个节点中
//	if idx == len(m.keys) {
//		idx = 0
//	}
//
//	return m.hashMap[m.keys[idx]]
//}
