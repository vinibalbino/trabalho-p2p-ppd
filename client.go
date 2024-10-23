package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// Função para servir arquivos que o cliente possui para outros clientes
func handleClientRequest(conn net.Conn) {
	defer conn.Close()

	// Lê o comando do cliente solicitante (esperando "DOWNLOAD <filename>")
	reader := bufio.NewReader(conn)
	request, err := reader.ReadString('\n')
	if err != nil {
		fmt.Println("Erro ao ler solicitação do cliente:", err)
		return
	}
	parts := strings.Split(strings.TrimSpace(request), " ")
	if len(parts) != 2 || parts[0] != "DOWNLOAD" {
		fmt.Fprintf(conn, "Comando inválido\n")
		return
	}
	fileName := parts[1]

	// Abre o arquivo solicitado
	file, err := os.Open(fileName)
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Arquivo '%s' não encontrado\n", fileName)
		return
	}
	defer file.Close()

	// Obtém o tamanho do arquivo
	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Fprintf(conn, "ERROR: Erro ao obter informações do arquivo\n")
		return
	}
	fileSize := fileInfo.Size()

	// Envia o tamanho do arquivo ao cliente solicitante
	fmt.Fprintf(conn, "%d\n", fileSize)

	// Envia o conteúdo do arquivo
	_, err = io.Copy(conn, file)
	if err != nil {
		fmt.Println("Erro ao enviar arquivo:", err)
		return
	}

	fmt.Printf("Arquivo '%s' enviado com sucesso para o cliente.\n", fileName)
}

func uploadFile(conn net.Conn, filePath string) error {
	baseFileName := filepath.Base(filePath)
	fmt.Fprintf(conn, "UPLOAD %s\n", baseFileName)
	fmt.Printf("Arquivo '%s' registrado no super nó.\n", baseFileName)
	return nil
}

func downloadFile(superNodeConn net.Conn, fileName string) error {
	// Solicita o download ao super nó
	fmt.Fprintf(superNodeConn, "DOWNLOAD %s\n", fileName)

	// Lê a resposta do super nó
	reader := bufio.NewReader(superNodeConn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("Erro ao ler a resposta do super nó: %v", err)
	}

	if strings.HasPrefix(response, "ERROR") {
		return fmt.Errorf(response) // Retorna o erro diretamente
	}

	// Verifica se a resposta está no formato correto antes de acessar índices
	parts := strings.Split(response, ": ")
	if len(parts) < 2 {
		return fmt.Errorf("Resposta inesperada do super nó: %s", response)
	}

	// Extrai o IP do cliente que possui o arquivo
	ipClient := strings.TrimSpace(parts[1])

	fmt.Printf("Iniciando download do arquivo '%s' do cliente %s\n", fileName, ipClient)

	// Conecta ao cliente que possui o arquivo
	clientConn, err := net.Dial("tcp", ipClient+":8082") // Porta onde o cliente está aguardando
	if err != nil {
		return fmt.Errorf("Erro ao conectar ao cliente: %v", err)
	}
	defer clientConn.Close()

	// Solicita o arquivo ao cliente
	fmt.Fprintf(clientConn, "DOWNLOAD %s\n", fileName)

	// Lê o tamanho do arquivo
	reader = bufio.NewReader(clientConn)
	fileSizeStr, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("Erro ao obter o tamanho do arquivo: %v", err)
	}

	fileSize, err := strconv.ParseInt(strings.TrimSpace(fileSizeStr), 10, 64)
	if err != nil {
		return fmt.Errorf("Erro ao converter o tamanho do arquivo: %v", err)
	}

	// Cria o arquivo localmente
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("Erro ao criar o arquivo local: %v", err)
	}
	defer file.Close()

	// Baixa o arquivo em blocos
	receivedBytes := int64(0)
	for receivedBytes < fileSize {
		n, err := io.CopyN(file, clientConn, 1024)
		if err != nil && err != io.EOF {
			return fmt.Errorf("Erro ao baixar o arquivo: %v", err)
		}
		receivedBytes += n
	}

	fmt.Printf("Download do arquivo '%s' concluído com sucesso.\n", fileName)
	return nil
}


func handleUserInteraction(superNodeConn net.Conn) {
	for {
		// Permite que o usuário faça várias requisições enquanto a conexão está aberta
		var choice int
		fmt.Println("\nEscolha uma opção: 1 - Upload | 2 - Download | 3 - Sair")
		fmt.Scan(&choice)

		if choice == 1 {
			fmt.Println("Digite o caminho do arquivo para upload:")
			var filePath string
			fmt.Scan(&filePath)
			err := uploadFile(superNodeConn, filePath)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("Upload registrado com sucesso.")
			}
		} else if choice == 2 {
			fmt.Println("Digite o nome do arquivo para download:")
			var fileName string
			fmt.Scan(&fileName)
			err := downloadFile(superNodeConn, fileName)
			if err != nil {
				fmt.Println(err)
			} else {
				fmt.Println("Download concluído com sucesso.")
			}
		} else if choice == 3 {
			fmt.Println("Fechando a conexão e saindo...")
			fmt.Fprintf(superNodeConn, "CLOSE")
			break
		} else {
			fmt.Println("Opção inválida")
		}
	}
}

func startClientServer() {
	ln, err := net.Listen("tcp", ":8082") // Escutando na porta 8082
	if err != nil {
		fmt.Println("Erro ao iniciar o servidor do cliente:", err)
		return
	}
	defer ln.Close()
	fmt.Println("Cliente está aguardando requisições na porta 8082...")

	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Erro ao aceitar conexão:", err)
			continue
		}

		go handleClientRequest(conn) // Lida com a requisição de outro cliente em uma goroutine
	}
}

func main() {
	// Inicia o servidor do cliente em uma goroutine
	go startClientServer()

	// Conecta ao super nó (mantém a conexão aberta)
	superNodeConn, err := net.Dial("tcp", "172.26.4.202:8081") // IP do super nó (modifique para o IP correto)
	if err != nil {
		fmt.Println("Erro ao conectar ao super nó:", err)
		return
	}
	defer superNodeConn.Close() // Conexão só será fechada quando o programa encerrar

	fmt.Println("Conexão estabelecida com o super nó.")

	// Inicia o loop de interação com o usuário
	handleUserInteraction(superNodeConn)
}
