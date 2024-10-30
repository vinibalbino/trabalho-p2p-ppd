// master_node.go
package main

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

type SuperNode struct {
	ID   int
	Addr string
}

var (
	superNodes     = make(map[int]SuperNode)
	contSuperNodes = 3
	contToSucess   = 0
	mu             sync.Mutex
)

// Porta para conexões de registro
const registerPort = ":8080"

// Porta para enviar mensagem de liberação
const releasePort = ":8081"

func handleSuperNodeRegistration(conn net.Conn, nodeId int) {
	defer conn.Close()

	// Extrai o endereço IP do SuperNode
	superNodeAddress := strings.Split(conn.RemoteAddr().String(), ":")[0]
	fmt.Printf("SuperNo: %d Addr: %s\n", nodeId, superNodeAddress)

	// Envia o ID do SuperNode como string
	_, err := fmt.Fprintf(conn, "%d", nodeId)
	if err != nil {
		fmt.Printf("Erro ao enviar nodeId para o SuperNode %d: %v\n", nodeId, err)
		return
	}

	// Guarda o endereço do SuperNode
	mu.Lock()
	superNodes[nodeId] = SuperNode{ID: nodeId, Addr: superNodeAddress}
	mu.Unlock()

	// Recebe confirmação do SuperNode
	buf := make([]byte, 1024)
	n, responseError := conn.Read(buf)
	if responseError != nil {
		fmt.Printf("Erro ao ler confirmação do SuperNode %d: %v\n", nodeId, responseError)
		return
	}

	message := strings.TrimSpace(string(buf[:n]))

	// Verifica se o SuperNode enviou "ACK"
	if message == "ACK" {
		mu.Lock()
		contToSucess++
		fmt.Printf("ACK recebido de NodeId %d\n", nodeId)
		// Se todos os SuperNodes confirmaram, libera a comunicação
		if contToSucess == 3 {
			go freeSuperNodes() // Executa freeSuperNodes em goroutine para evitar bloqueio
		}
		mu.Unlock()
	}
}

func freeSuperNodes() {
	time.Sleep(5 * time.Second)

	fmt.Println("Todos os super nós registrados. Liberando para comunicação...")

	mu.Lock()
	defer mu.Unlock()

	for _, superNode := range superNodes {
		conn, err := net.Dial("tcp", superNode.Addr+releasePort)
		if err != nil {
			fmt.Printf("Erro ao conectar ao SuperNode %d: %v\n", superNode.ID, err)
			continue
		}

		_, err = conn.Write([]byte("FINALIZED"))
		if err != nil {
			fmt.Printf("Erro ao enviar mensagem para SuperNode %d: %v\n", superNode.ID, err)
		} else {
			fmt.Printf("Mensagem de liberação enviada para SuperNode %d\n", superNode.ID)
		}
		_ = conn.Close()
	}
}

func main() {
	ln, err := net.Listen("tcp", registerPort)
	if err != nil {
		fmt.Println("Erro ao iniciar o servidor de registro:", err)
		return
	}
	defer ln.Close()

	fmt.Println("Nó coordenador aguardando registros dos super nós...")

	for contSuperNodes > 0 {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Erro ao aceitar conexão de registro:", err)
			continue
		}
		go handleSuperNodeRegistration(conn, contSuperNodes)
		contSuperNodes--
	}

	// Mantém o servidor ativo
	select {}
}
