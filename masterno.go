// master_node.go
package main

import (
	"fmt"
	"net"
	"sync"
)

type SuperNode struct {
	ID   string
	Addr string
}

var (
	superNodes = make(map[string]SuperNode)
	mu         sync.Mutex
)

func handleSuperNode(conn net.Conn) {
	defer conn.Close()

	var nodeID, nodeAddr string
	fmt.Fscan(conn, &nodeID, &nodeAddr)

	// Garantir exclusão mútua ao acessar o mapa
	mu.Lock()
	superNodes[nodeID] = SuperNode{ID: nodeID, Addr: nodeAddr}
	mu.Unlock()

	fmt.Println("SuperNode registrado:", nodeID, nodeAddr)
}

func main() {
	// Aumentar o backlog (limite de conexões pendentes) para um valor maior, ex: 100
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Erro ao iniciar o servidor:", err)
		return
	}
	defer ln.Close()

	fmt.Println("Nó coordenador aguardando super nós...")

	for {
		// Aceitar novas conexões
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Erro ao aceitar conexão:", err)
			continue
		}

		// Processar cada super nó em uma goroutine separada para suportar múltiplas conexões simultâneas
		go handleSuperNode(conn)
	}
}
