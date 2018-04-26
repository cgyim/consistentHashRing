package consistentHashRing

import (
	"fmt"
	"hash/crc32"
	"testing"
)

const MAXC = 4294967296

func TestNewConsistentHashRing(t *testing.T) {
	fmt.Println("TestNewConsistentHashRing....")
	testCase := []uint32{0, 1, MAXC - 1}
	ring1 := NewConsistentHashRing(crc32.ChecksumIEEE, 4, testCase)
	ring2 := NewConsistentHashRing(crc32.ChecksumIEEE, 8, testCase)

	expect1 := []uint32{
		0, 1,
		MAXC/4 - 1, MAXC / 4, MAXC/4 + 1,
		MAXC/2 - 1, MAXC / 2, MAXC/2 + 1,
		MAXC/4*3 - 1, MAXC / 4 * 3, MAXC/4*3 + 1, MAXC - 1}
	expect2 := []uint32{
		0, 1,
		MAXC/8 - 1, MAXC / 8, MAXC/8 + 1,
		MAXC/4 - 1, MAXC / 4, MAXC/4 + 1,
		MAXC/8*3 - 1, MAXC / 8 * 3, MAXC/8*3 + 1,
		MAXC/2 - 1, MAXC / 2, MAXC/2 + 1,
		MAXC/8*5 - 1, MAXC / 8 * 5, MAXC/8*5 + 1,
		MAXC/4*3 - 1, MAXC / 4 * 3, MAXC/4*3 + 1,
		MAXC/8*7 - 1, MAXC / 8 * 7, MAXC/8*7 + 1,
		MAXC - 1}
	for x, vnode1 := range ring1.virtualNodeSlice {
		if vnode1 != expect1[x] {
			t.Fatalf("ring1 : virtualnode error : expect %d but %d\n", expect1[x], vnode1)
		}
	}
	for y, vnode2 := range ring2.virtualNodeSlice {
		if vnode2 != expect2[y] {
			t.Fatalf("ring2 : virtualnode error : expect %d but %d\n", expect2[y], vnode2)
		}
	}
}

func TestSortedInsert(t *testing.T) {
	fmt.Println("TestSortedInsert....")
	testCase := []uint32{1, 2, 4, 8, 16, 32, 64, 100}
	result1 := SortedInsert(testCase, 40)
	if result1[6] != 40 {
		t.Fatalf("sorted insert error : expect %d but %d\n", 40, result1[6])
	}
	result1 = SortedInsert(result1, 0)
	result1 = SortedInsert(result1, 0)
	result1 = SortedInsert(result1, 100)
	result1 = SortedInsert(result1, 200)
	should_be := []uint32{0, 0, 1, 2, 4, 8, 16, 32, 40, 64, 100, 100, 200}
	for i, v := range result1 {
		if v != should_be[i] {
			t.Fatalf("sorted insert error : expect %d but %d\n", should_be[i], v)
		}
	}
}

func TestAddPhysicalNodes(t *testing.T) {
	testCase := []uint32{10000000, 20000000, 30000000, 40000000}
	fmt.Println("TestAddPhysicalNodes....")
	ring := NewConsistentHashRing(crc32.ChecksumIEEE, 4, testCase)
	ring.addPNode(100000000)
	ring.addPNode(999999999)
	ring.addPNode(1)
	should_be := []uint32{10000000, 20000000, 30000000, 40000000, 100000000, 999999999, 1}
	for i, pnode := range ring.PhysicalNodes() {
		if pnode != should_be[i] {
			t.Fatalf("ring : add physical nodes error : expect %d but %d\n", should_be[i], pnode)
		}
	}
}

func TestVirtualNodesAfterAddOrRemovePhysicalNodes(t *testing.T) {
	fmt.Println("TestVirtualNodesAfterAddOrRemovePhysicalNodes...")
	ring := NewConsistentHashRing(crc32.ChecksumIEEE, 4, []uint32{})
	if len(ring.PhysicalNodes()) != 0 {
		t.Fatalf("expect no nodes in ring, but %d\n", len(ring.PhysicalNodes()))
	}
	ring.addPNode(0)
	ring.addPNode(MAXC / 8)
	ring.addPNode(1)
	ring.addPNode(MAXC - 1)
	should_be := []uint32{
		0, 1, MAXC / 8,
		MAXC/4 - 1, MAXC / 4, MAXC/4 + 1, MAXC / 8 * 3,
		MAXC/2 - 1, MAXC / 2, MAXC/2 + 1, MAXC / 8 * 5,
		MAXC/4*3 - 1, MAXC / 4 * 3, MAXC/4*3 + 1, MAXC / 8 * 7, MAXC - 1}
	for i, node := range ring.VirtualNodes() {
		if node != should_be[i] {
			t.Fatalf("ring virtual node error: expect %d but %d\n", should_be[i], node)
		}
	}
	//remove 0 ,1
	remain := []uint32{
		MAXC / 8,
		MAXC/4 - 1, MAXC / 8 * 3,
		MAXC/2 - 1, MAXC / 8 * 5,
		MAXC/4*3 - 1, MAXC / 8 * 7, MAXC - 1,
	}
	ring.removePNode(0)
	ring.removePNode(1)

	for j, vnode := range ring.VirtualNodes() {
		if vnode != remain[j] {
			t.Fatalf("ring virtual node error: expect %d but %d\n", remain[j], vnode)
		}
	}

}

func TestObjectOperations(t *testing.T) {
	fmt.Println("TestObjectOperations...")
	testCase := []uint32{0, 123456789, MAXC / 8}
	ring := NewConsistentHashRing(crc32.ChecksumIEEE, 4, testCase)
	k1 := ring.add([]byte("1"))
	k2 := ring.add([]byte("test"))
	err, _, value1 := ring.get(k1)
	if err != nil {
		t.Fatalf("ring : get value err: expect key %d exist but failed\n", k1)
	}
	if string(value1) != "1" {
		t.Fatalf("ring : get value err: value not matched,expect %s but %s\n", "1", string(value1))
	}
	err, _, value2 := ring.get(k2)
	if err != nil {
		t.Fatalf("ring : get value err: expect key %d exist but failed\n", k2)
	}
	if string(value2) != "test" {
		t.Fatalf("ring : get value err: value not matched,expect %s but %s\n", "test", string(value2))
	}
}

func TestObjectAssignAfterAddOrRemoveNodes(t *testing.T) {
	fmt.Println("TestObjectAssignAfterAddOrRemoveNodes...")
	nodes := []uint32{0, MAXC / 16, MAXC / 8, MAXC / 16 * 3}
	var testCase, should_be []uint32
	for i := 1; i < 32; i += 2 {
		testCase = append(testCase, uint32(MAXC/32*i))
	}
	for k := 1; k <= 16; k++ {
		should_be = append(should_be, uint32(MAXC/16*(k%16)))
	}
	ring := NewConsistentHashRing(crc32.ChecksumIEEE, 4, nodes)
	for _, e := range testCase {
		ring.add([]byte(string(e)))
	}
	for j, b := range testCase {
		if ring.getObjectAssignNode(b) != should_be[j] {
			t.Fatalf("ring : objectkey %d assgin node err : expect %d but %d\n", b, should_be[j], ring.getObjectAssignNode(b))
		}
	}
	ring.removePNode(0)
	if ring.getObjectAssignNode(uint32(MAXC/32*31)) != MAXC/16 {
		t.Fatalf("ring : reassign %d after remove node %d err, expect %d but %d\n",
			uint32(MAXC/32*31), 0, MAXC/16, ring.getObjectAssignNode(uint32(MAXC/32*31)))
	}
	if ring.getObjectAssignNode(uint32(MAXC/32*15)) != MAXC/16*9 {
		t.Fatalf("ring : reassign %d after remove node %d err, expect %d but %d\n",
			uint32(MAXC/32*15), 0, MAXC/16*9, ring.getObjectAssignNode(uint32(MAXC/32*15)))
	}
	if ring.getObjectAssignNode(uint32(MAXC/32*7)) != MAXC/16*5 {
		t.Fatalf("ring : reassign %d after remove node %d err, expect %d but %d\n",
			uint32(MAXC/32*7), 0, MAXC/16*5, ring.getObjectAssignNode(uint32(MAXC/32*7)))
	}
	if ring.getObjectAssignNode(uint32(MAXC/32*23)) != MAXC/16*13 {
		t.Fatalf("ring : reassign %d after remove node %d err, expect %d but %d\n",
			uint32(MAXC/32*23), 0, MAXC/16*13, ring.getObjectAssignNode(uint32(MAXC/32*23)))
	}
	ring.addPNode(MAXC / 4)
	if ring.getObjectAssignNode(uint32(MAXC/32*31)) != 0 {
		t.Fatalf("add node back, expect %d but %d\n", 0, ring.getObjectAssignNode(uint32(MAXC/32*31)))
	}
}
