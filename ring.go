package consistentHashRing

import (
	"errors"
	"sort"
	"sync"
)

/*
consistent hashRing, with max length 2^32, safe for concurrent operations,
nodeNum is the number of physical nodes that objects assigned to. Default virtualNode factor is 8,
which means the actual number of virtual nodes in the hashRing is nodeNum * 4.
virtualFactor in order to balance the gathered distribution of objects.
*/

const MAXCAPACITY = 4294967296

type HashRing struct {
	ringData          map[uint32][]byte
	rwMu              sync.RWMutex
	keySlice          []uint32 //sorted
	hashFun           func([]byte) uint32
	virtualFactor     int
	physicalNodeSlice []uint32 //each physical node has virtualFactor times virtual nodes.
	virtualNodeSlice  []uint32 //to each physical node, virtualNode should be related.

}

//return a new HashRing
func NewConsistentHashRing(fn func([]byte) uint32, rep int, nodes []uint32) *HashRing {

	hashRing := &HashRing{
		ringData:          make(map[uint32][]byte),
		rwMu:              sync.RWMutex{},
		physicalNodeSlice: nodes,
		hashFun:           fn,
		virtualFactor:     rep,
	}
	/*
	  each physical node has virtualFactor times virtualNodes that map to the same physical node,
	  it balances the distribution of objects.
	*/

	for _, pNode := range nodes {
		cutRange := MAXCAPACITY / hashRing.virtualFactor

		//each virtualNode mapping to the same physical nodes cut the ring into virtualFactor parts.
		for i := 0; i < hashRing.virtualFactor; i++ {
			hashRing.virtualNodeSlice = append(hashRing.virtualNodeSlice,
				pNode%uint32(cutRange)+uint32(i*MAXCAPACITY/hashRing.virtualFactor))
		}
	}
	sort.Slice(hashRing.virtualNodeSlice, func(i, j int) bool {
		return hashRing.virtualNodeSlice[i] < hashRing.virtualNodeSlice[j]
	})
	return hashRing
}

/*
get the virtualNode that the object should assign to, should be the first virtualNode
on clockwise.
*/

func (ring *HashRing) getObjectAssignNode(objectHash uint32) (node uint32) {
	ring.rwMu.RLock()
	defer ring.rwMu.RUnlock()
	index := sort.Search(len(ring.virtualNodeSlice), func(i int) bool {
		return ring.virtualNodeSlice[i] >= objectHash
	})
	if index == len(ring.virtualNodeSlice) {
		return ring.virtualNodeSlice[0]
	}
	return ring.virtualNodeSlice[index]
}

/*
sorted insert, insert a element into a sorted slice,return the result slice.
*/
func SortedInsert(s []uint32, f uint32) []uint32 {
	l := len(s)
	if l == 0 {
		return append(s, f)
	}

	j := sort.Search(l, func(i int) bool { return s[i] >= f })
	if j == l { // not found = new value is the biggest
		return append(s, f)
	}

	return append(s[0:j], append([]uint32{f}, s[j:]...)...)
}

//add a object into ring
func (ring *HashRing) add(b []byte) uint32 {
	ring.rwMu.Lock()
	defer ring.rwMu.Unlock()
	hashKey := ring.hashFun(b)
	//ring.keySlice = append(ring.keySlice, hashKey)
	//sort.Slice(ring.keySlice, func(i, j int) bool {
	//	return ring.keySlice[i] < ring.keySlice[j]
	//})
	ring.keySlice = SortedInsert(ring.keySlice, hashKey)
	ring.ringData[hashKey] = b
	return hashKey
}

//get a object from ring by key, if key not exist, get an error.
func (ring *HashRing) get(key uint32) (error, int, []byte) {
	ring.rwMu.RLock()
	defer ring.rwMu.RUnlock()
	v, ok := ring.ringData[key]
	if !ok {
		return errors.New("key not found"), -1, nil
	}
	return nil, sort.Search(len(ring.keySlice), func(i int) bool {
		return ring.keySlice[i] >= key
	}), v
}

//delete an object from ring, if the object not exist, get an error.
func (ring *HashRing) delete(key uint32) error {
	ring.rwMu.Lock()
	defer ring.rwMu.Unlock()
	err, index, _ := ring.get(key)
	if err != nil {
		return errors.New("no Such hashKey")
	}
	ring.keySlice = append(ring.keySlice[:index], ring.keySlice[index+1:]...)
	delete(ring.ringData, key)
	return nil
}

//add a physical node into ring, while add virtual node accordingly
func (ring *HashRing) addPNode(pNode uint32) {
	ring.rwMu.Lock()
	defer ring.rwMu.Unlock()
	ring.physicalNodeSlice = append(ring.physicalNodeSlice, pNode)
	cutRange := MAXCAPACITY / ring.virtualFactor

	for i := 0; i < ring.virtualFactor; i++ {
		ring.virtualNodeSlice = SortedInsert(ring.virtualNodeSlice,
			pNode%uint32(cutRange)+uint32(i*MAXCAPACITY/ring.virtualFactor))
	}
}

//remove a physical node from ring, while remove virtual node accordingly
func (ring *HashRing) removePNode(node uint32) error {
	ring.rwMu.Lock()
	defer ring.rwMu.Unlock()
	cutRange := MAXCAPACITY / ring.virtualFactor
	if len(ring.physicalNodeSlice) == 0 {
		return errors.New("no nodes in ring")
	}
	for i, Pnode := range ring.physicalNodeSlice {
		if Pnode == node {
			//remove physical node
			ring.physicalNodeSlice = append(ring.physicalNodeSlice[:i], ring.physicalNodeSlice[i+1:]...)
			//remove all virtual node
			for k := 0; k < ring.virtualFactor; k++ {
				virtualNode := Pnode%uint32(cutRange) + uint32(k*MAXCAPACITY/ring.virtualFactor)
				vindex := sort.Search(len(ring.virtualNodeSlice), func(i int) bool {
					return ring.virtualNodeSlice[i] >= virtualNode
				})
				ring.virtualNodeSlice = append(ring.virtualNodeSlice[:vindex], ring.virtualNodeSlice[vindex+1:]...)
			}
			return nil
		}

	}
	return errors.New("no such node in ring")
}

func (ring *HashRing) PhysicalNodes() []uint32 {
	ring.rwMu.RLock()
	defer ring.rwMu.RUnlock()
	return ring.physicalNodeSlice
}

func (ring *HashRing) VirtualNodes() []uint32 {
	ring.rwMu.RLock()
	defer ring.rwMu.RUnlock()
	return ring.virtualNodeSlice
}

//func (ring *HashRing)DistributionInfo(node uint32) []uint32 {
//
//}
