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
	m, _ := node.NewLocal(4999, 7, 8, "")
	fmt.Println(4999)
	seed := "localhost:4999"
	h := make([]*node.Local, 32)
	h[0] = m
	for i := 0; i < 15; i++ {
		v, _ := node.NewLocal(i+5000, 2, 8, seed)
		fmt.Println(v.ID)
		h[i+1] = v
		//fmt.Println(err)
		//fmt.Println(v.Node.ID)
	}
	q, err := node.NewPeer(seed)
	for _, f := range h {
		if f != nil {
			node.PrintPeers(f)
		}
	}
	time.Sleep(2 * time.Second)
	for i := 0; i < 30; i++ {
		fmt.Println("yod")
		err := q.Set(strconv.Itoa(i), []byte(fmt.Sprint(i*8)))
		fmt.Println("yo")
		if err != nil {
			fmt.Println(err)
		}
	}
	//fmt.Println(err)
	for i := 0; i < 30; i++ {
		//q.Set(string(i), []byte(fmt.Sprint(i*8)))
		val, err := q.Get(strconv.Itoa(i))
		if err != nil {
			fmt.Println(err)
		}
		if v, err := strconv.Atoi(string(val)); v != i*8 && err == nil {
			fmt.Println(v, i*8)
		}
	}
	time.Sleep(2)
	for i := 0; i < 16; i++ {
		v, _ := node.NewLocal(i+5100, 6, 8, seed)
		h[i+16] = v
		fmt.Println(v.ID)
	}
	for _, f := range h {
		if f != nil {
			node.PrintPeers(f)
		}
	}
	time.Sleep(3 * time.Second)
	fmt.Println("Done Filling")
	start := time.Now()
	for i := 0; i < 30; i++ {
		//q.Set(string(i), []byte(fmt.Sprint(i*8)))
		time.Sleep(10 * time.Millisecond)
		_, err := q.Get(strconv.Itoa(i))
		fmt.Println(i, err, keyToID(strconv.Itoa(i), 7))
		for _, f := range h {
			if f != nil {
				if node.CheckItems(f, strconv.Itoa(i)) {
					fmt.Println("found at", f.ID)
				}
			}
		}
	}
	val, err := q.Get(strconv.Itoa(25))
	fmt.Println(string(val), err, time.Since(start))

}
