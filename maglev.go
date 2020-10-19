package maglev

import (
	"errors"
	"math/big"
	"sort"
	"sync"

	"github.com/dchest/siphash"
)

const (
	bigM uint64 = 65537
)

//Maglev 结构体，类似Java里Maglev的类
type Maglev struct {
	n           uint64     // size of VIP backends，实际后端服务器的数量？
	m           uint64     // size of lookup table，哈希表的数量
	permutation [][]uint64 // 排列
	lookup      []int64
	nodeList    []string
	lock        *sync.RWMutex
}

//NewMaglev 类似 Java里的构造函数
func NewMaglev(backends []string, m uint64) (*Maglev, error) {
	// 检测一个数字是否为素数
	if !big.NewInt(0).SetUint64(m).ProbablyPrime(1) {
		// 哈希表（look up table）的大小必须是一个 素数
		return nil, errors.New("Look up table size is not a prime number")
	}
	// 初始化，结构体与指针一样都是值传递，比如当把数组或结构体作为实参传给函数的形参时，会复制一个副本，所以为了提高性能，一般不会把数组直接传递给函数
	mag := &Maglev{m: m, lock: &sync.RWMutex{}}
	if err := mag.Set(backends); err != nil {
		return nil, err
	}
	return mag, nil
}

// Set: 设置后端服务器个数
func (m *Maglev) Set(backends []string) error {
	m.lock.Lock()
	defer m.lock.Unlock()

	n := uint64(len(backends))
	if m.m < n {
		return errors.New("Number of backends is greater than look up table")
	}
	// 后台服务器列表
	m.nodeList = make([]string, n)
	copy(m.nodeList, backends)
	m.n = n
	m.generatePopulation()
	m.populate()
	return nil
}

// 通过sipHash算法，把服务器名称依次hash，然后再排列
func (m *Maglev) generatePopulation() {
	m.permutation = nil
	//len(nodelist) 不就是n吗？
	if len(m.nodeList) == 0 {
		return
	}
	sort.Strings(m.nodeList)
	for i := 0; i < len(m.nodeList); i++ {
		bData := []byte(m.nodeList[i])

		offset := siphash.Hash(0xdeadbabe, 0, bData) % m.m
		skip := (siphash.Hash(0xdeadbeef, 0, bData) % (m.m - 1)) + 1

		iRow := make([]uint64, m.m)
		var j uint64
		for j = 0; j < m.m; j++ {
			iRow[j] = (offset + uint64(j)*skip) % m.m
		}
		// 排列  [][]uint64
		m.permutation = append(m.permutation, iRow)
	}
}

func (m *Maglev) populate() {
	if len(m.nodeList) == 0 {
		return
	}

	var i, j uint64
	next := make([]uint64, m.n)
	entry := make([]int64, m.m)
	for j = 0; j < m.m; j++ {
		entry[j] = -1
	}
	var n uint64
	for {
		for i = 0; i < m.n; i++ {
			c := m.permutation[i][next[i]]
			for entry[c] >= 0 {
				next[i] = next[i] + 1
				c = m.permutation[i][next[i]]
			}
			entry[c] = int64(i)
			next[i] = next[i] + 1
			n++
			if n == m.m {
				m.lookup = entry
				return
			}
		}
	}
}

// Add 新增一个后台服务器
func (m *Maglev) Add(backend string) error {
	m.lock.Lock()
	defer m.lock.Unlock()
	for _, v := range m.nodeList {
		if v == backend {
			return errors.New("Exist already")
		}
	}

	if m.m == m.n {
		return errors.New("Number of backends would be greater than lookup table")
	}

	m.nodeList = append(m.nodeList, backend)
	m.n = uint64(len(m.nodeList))
	m.generatePopulation()
	m.populate()
	return nil
}

// Remove 删除一个后台服务器
func (m *Maglev) Remove(backend string) error {
	// 类似于java里的synchronized
	m.lock.Lock()
	defer m.lock.Unlock()

	index := sort.SearchStrings(m.nodeList, backend)
	if index == len(m.nodeList) {
		return errors.New("Not Found")
	}
	m.nodeList = append(m.nodeList[:index], m.nodeList[index+1:]...)

	m.n = uint64(len(m.nodeList))
	m.generatePopulation()
	m.populate()
	return nil
}

//Get 给一个客户端，获取它对应的后台服务器
func (m *Maglev) Get(client string) (string, error) {
	m.lock.Lock()
	defer m.lock.Unlock()

	if len(m.nodeList) == 0 {
		return "", errors.New("Empty")
	}
	key := m.hashKey(client)
	return m.nodeList[m.lookup[key%m.m]], nil
}

// hashKey 计算服务器或者客户端的hash值
func (m *Maglev) hashKey(obj string) uint64 {
	return siphash.Hash(0xdeadbabe, 0, []byte(obj))
}

func (m *Maglev) Clear() {
	m.lock.Lock()
	defer m.lock.Unlock()

	m.nodeList = nil
	m.permutation = nil
	m.lookup = nil
}
