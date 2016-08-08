package node

import (
	"log"
	"sort"
	"sync"
)

type ByDistance struct {
	peers  []RPCNodeProxy
	target uint64
}

func SortByDistance(peers []RPCNodeProxy, target uint64) []RPCNodeProxy {
	peerscopy := make([]RPCNodeProxy, len(peers))
	copy(peerscopy, peers)
	sort.Sort(ByDistance{
		peers:  peerscopy,
		target: target,
	})
	return peerscopy
}

func (peers ByDistance) Len() int {
	return len(peers.peers)
}
func (peers ByDistance) Swap(i, j int) {
	temp := peers.peers[i]
	peers.peers[i] = peers.peers[j]
	peers.peers[j] = temp
}
func (peers ByDistance) Less(i, j int) bool {
	if peers.peers[i] == nil && peers.peers[j] == nil {
		return false
	} else if peers.peers[j] == nil {
		return true
	}
	return peers.peers[i] == nil || peers.peers[i].ID()^peers.target < peers.peers[j].ID()^peers.target
}

type Peers struct {
	sync.RWMutex
	center uint64
	peers  []RPCNodeProxy
	dims   uint
}

func NewPeers(center uint64, dims uint) *Peers {
	return &Peers{
		center: center,
		peers:  make([]RPCNodeProxy, dims),
		dims:   dims,
	}
}

func (peers *Peers) bitslot(key uint64) int {
	pos := -1
	min := ^uint64(0) >> 16

	for i, peer := range peers.peers {
		peers.RLock()

		if peer != nil {
			dist := key ^ (peers.center ^ (uint64(1) << uint(i)))
			if dist < min || pos == -1 {
				pos = i
				min = dist
			}
		}
		peers.RUnlock()
	}

	return pos

}

func (peers *Peers) GetPeers() []RPCNodeProxy {
	peers.RLock()
	peerscopy := make([]RPCNodeProxy, peers.dims)
	copy(peerscopy, peers.peers)
	peers.RUnlock()
	return peerscopy
}

func (peers *Peers) Get(index uint) RPCNodeProxy {
	peers.RLock()
	defer peers.RUnlock()
	return peers.peers[index]
}

func (peers *Peers) Place(newpeer RPCNodeProxy, index int) {
	peers.Lock()
	peers.add(peers.peers[index], index)
	peers.peers[index] = newpeer
	newpeer.Connect()
	peers.Unlock()
	return
}

func (peers *Peers) add(newpeer RPCNodeProxy, offset int) bool {
	if newpeer == nil {
		log.Printf("PeerNil %v", offset)
		return false
	}

	if offset < len(peers.peers) {
		for i := len(peers.peers) - 1 - offset; i >= 0; i-- {

			peer := peers.peers[i]

			mask := (uint64(1) << uint(i))
			if peer == nil {
				newpeer.Connect()
				peers.peers[i] = newpeer
				return true
			} else if peer.ID()^(peers.center^mask) > newpeer.ID()^(peers.center^mask) {

				peers.add(peers.peers[i], int(peers.dims)-i)
				newpeer.Connect()
				peers.peers[i] = newpeer

				//log.Println(peer.Addr, pos)

				return true

			} else if newpeer.ID() == peer.ID() {
				newpeer.Close()
				return false
			}
		}
	}
	newpeer.Close()
	return false

}

func (peers *Peers) GetClosest(key uint64) []RPCNodeProxy {
	peers.RLock()
	defer peers.RUnlock()
	return SortByDistance(peers.peers, key)
}

func (peers *Peers) Add(newpeer RPCNodeProxy) bool {
	peers.Lock()
	res := peers.add(newpeer, 0)
	peers.Unlock()
	return res
}
