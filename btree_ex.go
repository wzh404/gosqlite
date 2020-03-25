package gosqlite

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
)

const (
	pageSize          = 512
	rootPageNo uint32 = 1

	nodeTypeInternal byte = 0x01
	nodeTypeLeaf     byte = 0x02

	nodeUsed   byte = 0x01
	nodeUnused byte = 0x00

	offsetPageNo       = 0
	offsetNodeType     = 4
	offsetUsed         = 5
	offsetParent       = 8
	offsetUsablePtr    = 12
	offsetNumberOfKey  = 32
	offsetKey          = 36
	offsetPayload      = pageSize - 8
	offsetNext         = pageSize - 8
	offsetOverflowPage = pageSize - 4
)

// BPlusTree b+ tree
type BPlusTree struct {
	data  []byte
	leaf  uint32
	order int
}

func ceil(n int64) int {
	return int(math.Ceil(float64(n) / 2))
}

func blockCopy(src []byte, srcOffset int, dst []byte, dstOffset, count int) (bool, error) {
	srcLen := len(src)
	if srcOffset > srcLen || count > srcLen || srcOffset+count > srcLen {
		return false, errors.New("The source buffer index is out of range")
	}
	dstLen := len(dst)
	if dstOffset > dstLen || count > dstLen || dstOffset+count > dstLen {
		return false, errors.New("The destination buffer index is out of range")
	}
	index := 0
	for i := srcOffset; i < srcOffset+count; i++ {
		dst[dstOffset+index] = src[srcOffset+index]
		index++
	}
	return true, nil
}

func setInt32(data []byte, offset int, v uint32) {
	len := offset + 4
	binary.BigEndian.PutUint32(data[offset:len], v)
}

func getInt32(data []byte, offset int) uint32 {
	len := offset + 4
	return binary.BigEndian.Uint32(data[offset:len])
}

func setInt64(data []byte, offset int, v uint64) {
	len := offset + 8
	binary.BigEndian.PutUint64(data[offset:len], v)
}

func getInt64(data []byte, offset int) uint64 {
	len := offset + 8
	return binary.BigEndian.Uint64(data[offset:len])
}

func (b *BPlusTree) getPageInt32(page uint32, offset int) uint32 {
	data := b.getPageData(page)
	return getInt32(data, offset)
}

func (b *BPlusTree) setPageInt32(page uint32, offset int, v uint32) {
	data := b.getPageData(page)
	setInt32(data, offset, v)
}

func (b *BPlusTree) getPageData(page uint32) []byte {
	offset := page * pageSize
	len := offset + pageSize
	return b.data[offset:len]
}

func (b *BPlusTree) inc(page uint32) {
	numberOfKey := b.getNumberOfKey(page) + 1
	b.setNumberOfKey(page, numberOfKey)
}

func (b *BPlusTree) getMaxKey(page uint32) uint64 {
	numberOfKey := int(b.getNumberOfKey(page))
	return b.getKey(page, numberOfKey-1)
}

func (b *BPlusTree) dec(page uint32) {
	numberOfKey := b.getNumberOfKey(page) - 1
	b.setNumberOfKey(page, numberOfKey)
}

func (b *BPlusTree) setPageNo(page uint32, v uint32) {
	b.setPageInt32(page, offsetPageNo, v)
}

func (b *BPlusTree) getPageNo(page uint32) uint32 {
	return b.getPageInt32(page, offsetPageNo)
}

func (b *BPlusTree) setNext(page uint32, v uint32) {
	b.setPageInt32(page, offsetNext, v)
}

func (b *BPlusTree) getNext(page uint32) uint32 {
	return b.getPageInt32(page, offsetNext)
}

func (b *BPlusTree) setNumberOfKey(page uint32, v uint32) {
	b.setPageInt32(page, offsetNumberOfKey, v)
}

func (b *BPlusTree) getNumberOfKey(page uint32) uint32 {
	return b.getPageInt32(page, offsetNumberOfKey)
}

func (b *BPlusTree) setNodeType(page uint32, v byte) {
	data := b.getPageData(page)
	data[offsetNodeType] = v
}

func (b *BPlusTree) getNodeType(page uint32) byte {
	data := b.getPageData(page)
	return data[offsetNodeType]
}

func (b *BPlusTree) setUsed(page uint32, v byte) {
	data := b.getPageData(page)
	data[offsetUsed] = v
}

func (b *BPlusTree) isUsed(page uint32) bool {
	data := b.getPageData(page)
	return data[offsetUsed] == nodeUsed
}

func (b *BPlusTree) setParent(page uint32, v uint32) {
	b.setPageInt32(page, offsetParent, v)
}

func (b *BPlusTree) getParent(page uint32) uint32 {
	return b.getPageInt32(page, offsetParent)
}

func (b *BPlusTree) setKey(page uint32, index int, k uint64) {
	data := b.getPageData(page)
	offset := offsetKey + index*12
	setInt64(data, offset, k)
}

func (b *BPlusTree) getKey(page uint32, index int) uint64 {
	data := b.getPageData(page)
	offset := offsetKey + index*12
	return getInt64(data, offset)
}

func (b *BPlusTree) setCellPtr(page uint32, index int, k uint32) {
	data := b.getPageData(page)
	offset := offsetKey + index*12 + 8
	setInt32(data, offset, k)
}

func (b *BPlusTree) getCellPtr(page uint32, index int) uint32 {
	data := b.getPageData(page)
	offset := offsetKey + index*12 + 8
	return getInt32(data, offset)
}

func (b *BPlusTree) setUsablePtr(page uint32, ptr uint32) {
	data := b.getPageData(page)
	setInt32(data, offsetUsablePtr, ptr)
}

func (b *BPlusTree) getUsablePtr(page uint32) uint32 {
	data := b.getPageData(page)
	return getInt32(data, offsetUsablePtr)
}

func (b *BPlusTree) getKeyIndex(page uint32, key uint64) int {
	numberOfKey := int(b.getNumberOfKey(page))
	for i := 0; i < numberOfKey; i++ {
		ikey := b.getKey(page, i)
		if ikey == key {
			return i
		}
	}

	return -1
}

func (b *BPlusTree) getChildByIndex(page uint32, index int) uint32 {
	key := b.getKey(page, index)
	return b.getChildByKey(page, key)
}

func (b *BPlusTree) getChildByKey(page uint32, key uint64) uint32 {
	cell := b.getKeyCell(page, key)
	return binary.BigEndian.Uint32(cell)
}

func (b *BPlusTree) marshal(child uint32, payload []byte) []byte {
	if payload != nil {
		cell := make([]byte, 8+len(payload))
		binary.BigEndian.PutUint32(cell, child)
		binary.BigEndian.PutUint32(cell[4:], uint32(len(payload)))
		blockCopy(payload, 0, cell, 8, len(payload))
		return cell
	} else {
		cell := make([]byte, 4)
		binary.BigEndian.PutUint32(cell, child)
		return cell
	}
}

func lshift(data []byte, src int, len int, shiftSize int) {
	for i := 0; i < len; i++ {
		data[src-shiftSize+i] = data[src+i]
	}
}

func rshift(data []byte, src int, len int, shiftSize int) {
	for i := len - 1; i >= 0; i-- {
		data[src+shiftSize+i] = data[src+i]
	}
}

// shift data[src] of length len to data[dst]
func shift(data []byte, src int, len int, shiftSize int) {
	if shiftSize > 0 {
		rshift(data, src, len, shiftSize)
	} else if shiftSize < 0 {
		lshift(data, src, len, -shiftSize)
	}
}

func (b *BPlusTree) shiftCellPtr(page uint32, cellptr uint32, shiftSize int) {
	numberOfKey := int(b.getNumberOfKey(page))
	for i := 0; i < numberOfKey; i++ {
		icellPtr := b.getCellPtr(page, i)
		if icellPtr > 0 && icellPtr < cellptr {
			b.setCellPtr(page, i, uint32(int(icellPtr)+shiftSize))
		}
	}
}

func (b *BPlusTree) deleteCell(page uint32, index int) {
	cellptr := b.getCellPtr(page, index)
	if cellptr == 0 {
		return
	}

	data := b.getPageData(page)
	cell := b.getKeyCell(page, b.getKey(page, index))
	usablePtr := b.getUsablePtr(page)
	shiftSize := len(cell)
	length := int(cellptr - usablePtr)
	if length > 0 {
		shift(data, int(usablePtr), length, shiftSize)
		b.shiftCellPtr(page, cellptr, shiftSize)
	}
	b.setUsablePtr(page, uint32(int(usablePtr)+shiftSize))
	b.setCellPtr(page, index, 0)
}

func (b *BPlusTree) insertOrUpdateCell(page uint32, index int, cell []byte) {
	cellPtr := b.getCellPtr(page, index)
	data := b.getPageData(page)
	oldCell := b.getKeyCell(page, b.getKey(page, index))
	shiftSize := 0
	// insert cell
	if cellPtr == 0 {
		cellPtr = b.getUsablePtr(page) - uint32(len(cell))
		b.setCellPtr(page, index, uint32(cellPtr))
		b.setUsablePtr(page, cellPtr)
	} else if oldCell != nil { // update cell
		shiftSize = len(oldCell) - len(cell)
		usablePtr := b.getUsablePtr(page)
		len := int(cellPtr - usablePtr)
		if len > 0 {
			shift(data, int(usablePtr), len, shiftSize)
			b.shiftCellPtr(page, cellPtr, shiftSize)
			b.setUsablePtr(page, uint32(int(usablePtr)+shiftSize))
			b.setCellPtr(page, index, uint32(int(cellPtr)+shiftSize))
		}
	}
	blockCopy(cell, 0, data, int(cellPtr)+shiftSize, len(cell))
}

func (b *BPlusTree) getKeyCell(page uint32, key uint64) []byte {
	index := b.getKeyIndex(page, key)
	if index == -1 {
		return nil
	}
	offset := b.getCellPtr(page, index)
	if b.getNodeType(page) == nodeTypeLeaf {
		data := b.getPageData(page)
		payloadSize := binary.BigEndian.Uint32(data[offset+4:])
		cell := make([]byte, 8+payloadSize)

		blockCopy(data, int(offset), cell, 0, len(cell))
		return cell
	} else {
		cell := make([]byte, 4)

		data := b.getPageData(page)
		blockCopy(data, int(offset), cell, 0, len(cell))
		return cell
	}
}

func (b *BPlusTree) getKeyPayload(page uint32, key uint64) []byte {
	cell := b.getKeyCell(page, key)
	if b.getNodeType(page) == nodeTypeLeaf {
		return cell[8:]
	}
	return nil
}

func (b *BPlusTree) setChild(page uint32, index int, child uint32, payload []byte) {
	if payload == nil {
		cell := make([]byte, 4)
		binary.BigEndian.PutUint32(cell, child)
		b.insertOrUpdateCell(page, index, cell)
	} else {
		cell := b.marshal(child, payload)
		b.insertOrUpdateCell(page, index, cell)
	}
}

func (b *BPlusTree) updateChild(page uint32, index int, child uint32) {
	cell := make([]byte, 4)
	binary.BigEndian.PutUint32(cell, child)
	b.insertOrUpdateCell(page, index, cell)
}

func (b *BPlusTree) getChild(page uint32, index int) uint32 {
	return b.getChildByIndex(page, index)
}

func (b *BPlusTree) setChildParent(page uint32) {
	numberOfKey := int(b.getNumberOfKey(page))
	for i := 0; i < numberOfKey; i++ {
		ichild := b.getChild(page, i)
		b.setParent(ichild, page)
	}
}

func (b *BPlusTree) allocte() uint32 {
	for i := 2; i < 32; i++ {
		if !b.isUsed(uint32(i)) {
			b.setUsed(uint32(i), nodeUsed)
			return uint32(i)
		}
	}

	return 0
}

func (b *BPlusTree) copy(src uint32, dst uint32) {
	srcData := b.getPageData(src)
	dstData := b.getPageData(dst)

	copy(dstData, srcData)
	b.setPageNo(dst, dst)
}

func (b *BPlusTree) search(key uint64) uint32 {
	if b.getNodeType(rootPageNo) == nodeTypeLeaf {
		return rootPageNo
	}

	return b.searchInternalNode(rootPageNo, key)
}

// RangeSearch to search key from key1 to key2
func (b *BPlusTree) RangeSearch(key1 uint64, key2 uint64) {
	startPage := b.search(key1)
	endPage := b.search(key2)

	page := startPage
	for {
		numberOfKey := int(b.getNumberOfKey(page))
		for i := 0; i < numberOfKey; i++ {
			ikey := b.getKey(page, i)
			if ikey >= key1 && ikey <= key2 {
				fmt.Printf("%d ", ikey)
			}
		}
		if page == endPage {
			break
		} else {
			page = b.getNext(page)
		}
	}
}

// Write to write b+ tree to file
func (b *BPlusTree) Write(fileName string) {
	setInt32(b.data, 0, uint32(b.order))
	setInt32(b.data, 4, b.leaf)
	ioutil.WriteFile(fileName, b.data, 777)
}

func (b *BPlusTree) searchInternalNode(pageNo uint32, key uint64) uint32 {
	numberOfKey := int(b.getNumberOfKey(pageNo))
	k := numberOfKey - 1
	for i := 0; i < numberOfKey; i++ {
		nodeKey := b.getKey(pageNo, i)
		if nodeKey >= key {
			k = i
			break
		}
	}

	child := b.getChild(pageNo, k)
	if b.getNodeType(child) == nodeTypeLeaf {
		return child
	}
	return b.searchInternalNode(child, key)
}

func (b *BPlusTree) updateKey(pageNo uint32, oldKey uint64, newKey uint64) {
	numberOfKey := int(b.getNumberOfKey(pageNo))
	for i := 0; i < numberOfKey; i++ {
		if b.getKey(pageNo, i) == oldKey {
			b.setKey(pageNo, i, newKey)
		}
	}
}

func (b *BPlusTree) insertAndNotSplit(pageNo uint32, key uint64, child uint32, payload []byte) {
	numberOfKey := int(b.getNumberOfKey(pageNo))
	oldMaxKey := b.getKey(pageNo, numberOfKey-1)
	k := numberOfKey
	for i := numberOfKey - 1; i >= 0; i-- {
		ikey := b.getKey(pageNo, i)
		icellptr := b.getCellPtr(pageNo, i)
		if ikey < key {
			k = i + 1
			break
		} else {
			b.setKey(pageNo, i+1, ikey)
			b.setCellPtr(pageNo, i+1, icellptr)
			b.setCellPtr(pageNo, i, 0)
			k = i
		}
	}
	b.setKey(pageNo, k, key)
	b.setChild(pageNo, k, child, payload)

	newMaxKey := b.getKey(pageNo, numberOfKey)
	parent := b.getParent(pageNo)
	if parent != 0 && oldMaxKey != newMaxKey {
		b.updateKey(parent, oldMaxKey, newMaxKey)
	}
	b.setNumberOfKey(pageNo, uint32(numberOfKey+1))
}

func (b *BPlusTree) insertAndSplitRoot(root uint32, leftPage uint32, rightPage uint32) {
	// set root node
	leftMaxKey := b.getMaxKey(leftPage)
	rightMaxKey := b.getMaxKey(rightPage)
	b.setNodeType(root, nodeTypeInternal)
	b.setUsablePtr(root, offsetPayload)

	b.setKey(root, 0, leftMaxKey)
	b.setCellPtr(root, 0, 0)
	b.setChild(root, 0, leftPage, nil)

	b.setKey(root, 1, rightMaxKey)
	b.setCellPtr(root, 1, 0)
	b.setChild(root, 1, rightPage, nil)
	b.setNumberOfKey(root, 2)

	b.setParent(leftPage, root)
	b.setParent(rightPage, root)

	if b.getNodeType(leftPage) == nodeTypeLeaf {
		b.leaf = leftPage
	}
}

func (b *BPlusTree) insertAndSplitKey(pageNo uint32, key uint64, child uint32, payload []byte) uint32 {
	rightPageNo := b.allocte()
	leftNumberOfKey := ceil(int64(b.order))
	rightNumberOfKey := b.order - leftNumberOfKey + 1

	l, r, k := leftNumberOfKey, rightNumberOfKey, key
	for i := b.order - 1; i >= 0; i-- {
		ikey := b.getKey(pageNo, i)
		ichild := b.getChild(pageNo, i)
		ipayload := b.getKeyPayload(pageNo, ikey)
		icellptr := b.getCellPtr(pageNo, i)
		if k > ikey {
			if r > 0 {
				r = r - 1
				b.setKey(rightPageNo, r, key)
				b.setChild(rightPageNo, r, child, payload)
			} else {
				l = l - 1
				b.setKey(pageNo, l, key)
				b.setChild(pageNo, l, child, payload)
			}
			k = 0
		}

		if r > 0 {
			r = r - 1
			b.setKey(rightPageNo, r, ikey)
			b.setChild(rightPageNo, r, ichild, ipayload)
			b.setKey(pageNo, i, 0)
			b.deleteCell(pageNo, i)
		} else {
			l = l - 1
			b.setKey(pageNo, l, ikey)
			b.setCellPtr(pageNo, l, icellptr)
		}
	}
	if k != 0 {
		b.setKey(pageNo, 0, key)
		b.setChild(pageNo, 0, child, payload)
	}
	b.setNumberOfKey(rightPageNo, uint32(rightNumberOfKey))
	b.setNumberOfKey(pageNo, uint32(leftNumberOfKey))
	b.setNext(pageNo, rightPageNo)

	return rightPageNo
}

// TODO add child parameter,
func (b *BPlusTree) insertAndsplit(pageNo uint32, key uint64, child uint32, payload []byte) {
	oldLeftMaxKey := b.getMaxKey(pageNo)

	rightPageNo := b.insertAndSplitKey(pageNo, key, child, payload)
	splitNodeType := b.getNodeType(pageNo)
	b.setNodeType(rightPageNo, splitNodeType)

	parent := b.getParent(pageNo)
	if parent == 0 {
		// parent is root, The root node cannot be changed
		// 1. create left node and copy parent's data to left
		// 2. insert left and right to parent
		newLeftPage := b.allocte()
		b.copy(pageNo, newLeftPage)
		b.setNodeType(newLeftPage, splitNodeType)
		b.insertAndSplitRoot(pageNo, newLeftPage, rightPageNo)
		b.setChildParent(newLeftPage)
		b.setChildParent(rightPageNo)
	} else {
		// change parent node after split
		newLeftMaxKey := b.getMaxKey(pageNo)
		rightMaxKey := b.getMaxKey(rightPageNo)
		b.setParent(rightPageNo, parent)
		b.setChildParent(rightPageNo)
		// update parent's left key
		b.updateKey(parent, oldLeftMaxKey, newLeftMaxKey)
		// insert right node to parent
		b.insertKey(parent, rightMaxKey, rightPageNo, nil)
	}
}

// Insert to insert payload to b+ tree
func (b *BPlusTree) Insert(key uint64, payload []byte) {
	// search leaf node
	pageNo := b.search(key)
	b.insertKey(pageNo, key, 0, payload)
}

// Get to get payload from b+ tree
func (b *BPlusTree) Get(key uint64) []byte {
	page := b.search(key)
	return b.getKeyPayload(page, key)
}

func (b *BPlusTree) insertKey(pageNo uint32, key uint64, child uint32, payload []byte) {
	numberOfKey := b.getNumberOfKey(pageNo)
	if numberOfKey != uint32(b.order) {
		b.insertAndNotSplit(pageNo, key, child, payload)
	} else {
		b.insertAndsplit(pageNo, key, child, payload)
	}
}

func (b *BPlusTree) printKey(pageNo uint32) {
	numberOfKey := int(b.getNumberOfKey(pageNo))
	parent := b.getParent(pageNo)
	next := b.getNext(pageNo)

	fmt.Printf("[%d:P%d:N%d] -> ", pageNo, parent, next)
	for i := 0; i < numberOfKey; i++ {
		ikey := b.getKey(pageNo, i)
		ichild := b.getChild(pageNo, i)
		icellPtr := b.getCellPtr(pageNo, i)
		fmt.Printf("%d:C%d*[%d]:ptr[%d] |  ", i, ichild, ikey, icellPtr)
	}

	fmt.Println()
}

func (b *BPlusTree) printInternalNode(pageNo uint32) {
	b.printKey(pageNo)
	numberOfKey := int(b.getNumberOfKey(pageNo))
	for i := 0; i < numberOfKey; i++ {
		c := b.getChild(pageNo, i)
		ct := b.getNodeType(c)
		if ct == nodeTypeInternal {
			fmt.Printf("internal: ")
			b.printInternalNode(c)
		} else {
			fmt.Printf("leaf: ")
			b.printKey(c)
		}
	}
}

// Print to print b+ tree
func (b *BPlusTree) Print() {
	fmt.Printf("leaf is %d.\n", b.leaf)
	t := b.getNodeType(rootPageNo)
	if t == nodeTypeLeaf {
		b.printKey(rootPageNo)
	} else {
		b.printInternalNode(rootPageNo)
	}
}

// LoadBtree load data to bplustree
func LoadBtree(fileName string) *BPlusTree {
	tree := new(BPlusTree)

	data, err := ioutil.ReadFile("db0.log")
	if err != nil {
		return nil
	}
	tree.data = data
	tree.order = int(getInt32(data, 0))
	tree.leaf = getInt32(data, 4)

	return tree
}

// CreateTree to create b+ tree with order
func CreateTree(order int) *BPlusTree {
	tree := new(BPlusTree)
	tree.order = order
	tree.data = make([]byte, pageSize*32)
	for i := 0; i < 32; i++ {
		tree.setPageNo(uint32(i), uint32(i))
		tree.setUsed(uint32(i), nodeUnused)
		tree.setUsablePtr(uint32(i), offsetPayload)
	}
	tree.leaf = rootPageNo
	tree.setNodeType(rootPageNo, nodeTypeLeaf)
	tree.setUsed(rootPageNo, nodeUsed)

	return tree
}
