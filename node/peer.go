package node

import (
	"encoding/gob"
	"fmt"
	"net"
	"net/rpc"

	"github.com/rhino1998/hypercube/common"
)

//Registers types to be serialized
func init() {
	gob.Register(&Peer{})
	gob.Register(Node{})
	gob.Register(&FindMsg{})
}

//Peer is an RPCProxy to a remote node
type Peer struct {
	Node
	proxy *rpc.Client
	alive bool
}

//NewPeer initializes a remote proxy to a node given its attributes
//Local Method
func NewPeer(addr string) (*Peer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	proxy := rpc.NewClient(conn)

	var node Node
	err = proxy.Call("RPCNode.Info", &struct{}{}, &node)
	if err != nil {
		return nil, err
	}
	return &Peer{
		Node:  node,
		proxy: proxy,
		alive: true,
	}, err
}

func makePeer(addr string, id uint64) (*Peer, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	proxy := rpc.NewClient(conn)

	var node Node
	err = proxy.Call("RPCNode.Info", &struct{}{}, &node)
	if err != nil {
		return nil, err
	}
	remote := &Peer{
		Node:  node,
		proxy: proxy,
		alive: true,
	}
	remote.Node.ID = id
	return remote, err
}

//Connect establishes a connection to the remote node
//Local Method
func (remote *Peer) Connect() error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%v", remote.Node.IP, remote.Node.Port))
	if err != nil {
		return err
	}
	remote.proxy = rpc.NewClient(conn)
	remote.alive = true
	return nil
}

//GetDims the number of dimensions of the dht according to the node questioned
//RPC Method
func (remote *Peer) GetDims() (dims uint, err error) {
	err = remote.proxy.Call("RPCNode.GetDims", &struct{}{}, &dims)
	return dims, err
}

//Info returns the node attributes for the remote node represented by this proxy
//Local Method
func (remote *Peer) Info() Node {
	return remote.Node
}

//ID returns id of remote node
//Local Method
func (remote *Peer) ID() uint64 {
	return remote.Node.ID
}

//IP return ip address of remote node
//Local Method
func (remote *Peer) IP() string {
	return remote.Node.IP
}

//Port returns port of remote node
//RPC Proxy Method
func (remote *Peer) Port() int {
	return remote.Node.Port
}

//IsAlive checks wether remote node is alive
//RPC Proxy Method
func (remote *Peer) IsAlive() bool {
	return remote.alive
}

//Get key from remote node
//RPC Proxy Method
func (remote *Peer) Get(key string) (data []byte, err error) {
	err = remote.proxy.Call("RPCNode.Get", &key, &data)
	return data, err
}

//Set key at remote node
//RPC Proxy Method
func (remote *Peer) Set(key string, data []byte) error {
	return remote.proxy.Call("RPCNode.Set", &common.Item{key, data}, nil)
}

//Relocate moves a key to the optimal location
//RPC Proxy Method
func (remote *Peer) Relocate(key string, data []byte) error {
	return remote.proxy.Call("RPCNode.Relocate", &common.Item{key, data}, nil)
}

//Del key from remote node
//RPC Proxy Method
func (remote *Peer) Del(key string) error {
	return remote.proxy.Call("RPCNode.Del", &key, nil)
}

//GetPeers from remote node
//RPC Proxy Method
func (remote *Peer) GetPeers() (peers []RPCNodeProxy, err error) {
	err = remote.proxy.Call("RPCNode.GetPeers", &struct{}{}, &peers)
	return peers, err
}

//Ping remote node to ensure liveliness
//RPC Proxy Method
func (remote *Peer) Ping() error {
	return remote.proxy.Call("RPCNode.Pong", nil, nil)
}

//AssistBootstrap returns the optimal ID for the given node
//RPC Proxy Method
func (remote *Peer) AssistBootstrap(addr string) (id uint64, err error) {
	err = remote.proxy.Call("RPCNode.AssistBootstrap", &addr, &id)
	//fmt.Println(err)
	return id, err
}

//FindPeer returns the optimal peer for the given node
//RPC Proxy Method
func (remote *Peer) FindPeer(localNode *Node, id uint64) (peerNode Node, err error) {
	err = remote.proxy.Call("RPCNode.FindPeer", &FindMsg{Target: id, Node: localNode}, &peerNode)
	return peerNode, err
}

//Close closes the rpc proxy handler
//RPC Proxy Method
func (remote *Peer) Close() error {
	defer func() { recover() }()
	return remote.proxy.Close()
}
