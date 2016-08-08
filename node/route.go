package node

import "github.com/spaolacci/murmur3"

//keyToID hashes a key string into a uint64 with n bits
func keyToID(key string, n uint) uint64 {
	//SomeHash
	return murmur3.Sum64([]byte(key)) >> (uint(64 - n))
}

//isCloser determines whether or not an id is closer than another id to the target id
func isCloser(local, remote, keyid uint64) bool {
	return (local ^ keyid) > (remote ^ keyid)
}

//getip is a placeholder for ip initialization
func getip() string {
	return "localhost"
}

//log2 returns the integer log base 2 of a  uint64
func log2(val uint64) uint {
	result := uint(0)
	for val != 0 {
		val = val >> 1
		result++
	}
	return result
}

//num1 returns the number of on bits in a uint64
func num1(n uint64) uint {
	r := uint(0)
	for n != 0 {
		if n&1 != 0 {
			r++
		}
		n >>= 1
	}
	return r
}

//lead0 returns the number of trailing 0's (from the right) in a uint64
func trail0(n uint64) uint {
	r := uint(0)
	for n>>1<<1 == n && n != 0 {
		n >>= 1
		r++
	}
	return r
}

//nextPeer returns the next neighbor according to the sequence
//0, 1, 3, 2, 6, 7, 5, 4, 12, 13, 15, 14, 10, 8, ...
func nextPeer(id uint64) uint {
	if id == 0 {
		return 1
	}
	if (id & (id - 1)) == 0 {
		return log2(id) + 1
	}
	if num1(id)&1 != 0 {
		return 2 + trail0(id)
	}
	return 1
}
