package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/rhino1998/hypercube/node"
	"github.com/spaolacci/murmur3"
)

func keyToID(key string, n int) uint64 {
	//SomeHash
	return murmur3.Sum64([]byte(key)) >> (uint(64 - n))
}

func main() {
	node.NewLocal(4999, 7, 8, "")
	fmt.Println(4999)
	seed := "localhost:4999"
	for i := 0; i < 72; i++ {
		node.NewLocal(i+5000, 2, 8, seed)
		//fmt.Println(err)
		//fmt.Println(v.Node.ID)
	}
	q, err := node.NewNeighbor(seed)
	time.Sleep(2 * time.Second)
	for i := 0; i < 30; i++ {
		err := q.Set(strconv.Itoa(i), []byte(fmt.Sprint(i*8)))
		if err != nil {
			fmt.Println(err)
		}
	}
	//fmt.Println(err)
	for i := 0; i < 30; i++ {
		//q.Set(string(i), []byte(fmt.Sprint(i*8)))
		_, err := q.Get(strconv.Itoa(i))
		if err != nil {
			fmt.Println(err)
		}
	}
	time.Sleep(2)
	for i := 0; i < 22; i++ {
		go node.NewLocal(i+5100, 6, 8, seed)
	}
	time.Sleep(12 * time.Second)
	fmt.Println("Done Filling")
	start := time.Now()
	for i := 0; i < 30; i++ {
		fmt.Println(strconv.Itoa(i))
		//q.Set(string(i), []byte(fmt.Sprint(i*8)))
		time.Sleep(10 * time.Millisecond)
		val, err := q.Get(strconv.Itoa(i))
		fmt.Println(string(val), err, keyToID(strconv.Itoa(i), 7))
	}
	val, err := q.Get(strconv.Itoa(25))
	fmt.Println(string(val), err, time.Since(start))

}
