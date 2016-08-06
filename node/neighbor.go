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
	gob.Register(&Neighbor{})
	gob.Register(Node{})
	gob.Register(&FindMsg{})
}

//Neighbor is an RPCProxy to a remote node
type Neighbor struct {
	Node
	proxy *rpc.Client
	alive bool
}

//NewNeighbor initializes a remote proxy to a node given its attributes
//Local Method
func NewNeighbor(addr string) (*Neighbor, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	proxy := rpc.NewClient(conn)

	var node Node
	err = proxy.Call("RPCNode.Info", struct{}{}, &node)
	if err != nil {
		return nil, err
	}
	return &Neighbor{
		Node:  node,
		proxy: proxy,
		alive: true,
	}, err
}

//Connect establishes a connection to the remote node
//Local Method
func (remote *Neighbor) Connect() error {
	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%v", remote.Node.IP, remote.Node.Port))
	if err != nil {
		return err
	}
	remote.proxy = rpc.NewClient(conn)
	remote.alive = true
	return nil
}

func (remote *Neighbor) GetDims() (dims uint, err error) {
	err = remote.proxy.Call("RPCNode.Get", struct{}{}, &dims)
	return dims, err
}

//Info returns the node attributes for the remote node represented by this proxy
//Local Method
func (remote *Neighbor) Info() Node {
	return remote.Node
}

//ID returns id of remote node
//Local Method
func (remote *Neighbor) ID() uint64 {
	return remote.Node.ID
}

//IP return ip address of remote node
//Local Method
func (remote *Neighbor) IP() string {
	return remote.Node.IP
}

//Port returns port of remote node
//RPC Proxy Method
func (remote *Neighbor) Port() int {
	return remote.Node.Port
}

//IsAlive checks wether remote node is alive
//RPC Proxy Method
func (remote *Neighbor) IsAlive() bool {
	return remote.alive
}

//Get key from remote node
//RPC Proxy Method
func (remote *Neighbor) Get(key string) (data []byte, err error) {
	err = remote.proxy.Call("RPCNode.Get", &key, &data)
	return data, err
}

//Set key at remote node
//RPC Proxy Method
func (remote *Neighbor) Set(key string, data []byte) error {
	return remote.proxy.Call("RPCNode.Set", &common.Item{key, data}, nil)
}

//Relocate moves a key to the optimal location
//RPC Proxy Method
func (remote *Neighbor) Relocate(key string, data []byte) error {
	return remote.proxy.Call("RPCNode.Set", &common.Item{key, data}, nil)
}

//Del key from remote node
//RPC Proxy Method
func (remote *Neighbor) Del(key string) error {
	return remote.proxy.Call("RPCNode.Del", &key, nil)
}

//GetNeighbors from remote node
//RPC Proxy Method
func (remote *Neighbor) GetNeighbors() (neighbors []RPCNodeProxy, err error) {
	err = remote.proxy.Call("RPCNode.GetNeighbors", struct{}{}, &neighbors)
	return neighbors, err
}

//Ping remote node to ensure liveliness
//RPC Proxy Method
func (remote *Neighbor) Ping() error {
	return remote.proxy.Call("RPCNode.Pong", nil, nil)
}

//AssistBootstrap returns the optimal ID for the given node
//RPC Proxy Method
func (remote *Neighbor) AssistBootstrap(addr string) (id uint64, err error) {
	err = remote.proxy.Call("RPCNode.AssistBootstrap", &addr, &id)
	//fmt.Println(err)
	return id, err
}

//FindNeighbor returns the optimal neighbor for the given node
//RPC Proxy Method
func (remote *Neighbor) FindNeighbor(localNode *Node, id uint64) (neighborNode Node, err error) {
	err = remote.proxy.Call("RPCNode.FindNeighbor", &FindMsg{Target: id, Node: localNode}, &neighborNode)
	return neighborNode, err
}

//Close closes the rpc proxy handler
//RPC Proxy Method
func (remote *Neighbor) Close() error {
	defer func() { recover() }()
	return remote.proxy.Close()
}
