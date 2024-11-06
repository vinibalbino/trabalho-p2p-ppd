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
	contSuperNodes = 0
	contToSucess   = 0
	mu             sync.Mutex
)

// Porta para conexões de registro
const registerPort = ":8080"

// Porta para enviar mensagem de liberação
const releasePort = ":8081"

const broadcastPort = ":8084"

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
		superNodes[nodeId] = SuperNode{ID: nodeId, Addr: superNodeAddress}
		fmt.Printf("ACK recebido de NodeId %d\n", nodeId)
		// Se todos os SuperNodes confirmaram, libera a comunicação
		mu.Unlock()
	}
}

func freeNode(superNode SuperNode) {
	conn, err := net.Dial("tcp", superNode.Addr+releasePort)
	if err != nil {
		fmt.Printf("Erro ao conectar ao SuperNode %d: %v\n", superNode.ID, err)
		return
	}

	_, err = conn.Write([]byte("FINALIZED"))
	if err != nil {
		fmt.Printf("Erro ao enviar mensagem para SuperNode %d: %v\n", superNode.ID, err)
	} else {
		fmt.Printf("Mensagem de liberação enviada para SuperNode %d\n", superNode.ID)
	}
	_ = conn.Close()
}

func freeSuperNodes() {
	time.Sleep(5 * time.Second)

	fmt.Println("Todos os super nós registrados. Liberando para comunicação...")

	mu.Lock()
	defer mu.Unlock()

	for _, superNode := range superNodes {
		freeNode(superNode)
	}
}

func addressAlreadyExist(address string) bool {
	for _, node := range superNodes {
		if node.Addr == address {
			return true
		}
	}
	return false
}

func broadcastSuperNodes() {
	time.Sleep(2 * time.Second)
	mu.Lock()

	for _, superNode := range superNodes {
		conn, err := net.Dial("tcp", superNode.Addr+broadcastPort)

		if err != nil {
			fmt.Printf("Erro ao conectar ao SuperNode %d para enviar broadcast: %v\n", superNode.ID, err)
			continue
		}

		// Cria uma lista dos endereços IP de todos os super nós, separada por vírgulas
		var nodeList []string
		for _, node := range superNodes {
			nodeList = append(nodeList, node.Addr)
		}

		// Envia a lista de super nós para o super nó atual
		_, err = conn.Write([]byte(strings.Join(nodeList, ",")))
		if err != nil {
			fmt.Printf("Erro ao enviar lista para SuperNode %d: %v\n", superNode.ID, err)
		}

		_ = conn.Close()
	}
	mu.Unlock()
}

func main() {
	ln, err := net.Listen("tcp", registerPort)
	if err != nil {
		fmt.Println("Erro ao iniciar o servidor de registro:", err)
		return
	}

	fmt.Println("Nó coordenador aguardando registros dos super nós...")

	for contSuperNodes < 3 {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Erro ao aceitar conexão de registro:", err)
			continue
		}
		go handleSuperNodeRegistration(conn, contSuperNodes)
		contSuperNodes++
	}

	go freeSuperNodes() // Executa freeSuperNodes em goroutine para evitar bloqueio
	time.Sleep(6 * time.Second)
	go broadcastSuperNodes()

	defer ln.Close()
	for {
		time.Sleep(1 * time.Second)
		conn, err := ln.Accept()
		nodeAddress := strings.Split(conn.RemoteAddr().String(), ":")[0]
		exist := addressAlreadyExist(nodeAddress)
		if !exist {
			if err != nil {
				fmt.Println("Erro ao aceitar conexão de registro:", err)
				continue
			}
			go handleSuperNodeRegistration(conn, contSuperNodes)
			time.Sleep(5 * time.Second)
			freeNode(superNodes[contSuperNodes])
			contSuperNodes++
			broadcastSuperNodes()
		}
	}

}
