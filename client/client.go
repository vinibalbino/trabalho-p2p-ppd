// client.go
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

func uploadFile(conn net.Conn, filePath string) error {
	// Extrair apenas o nome do arquivo ao fazer upload
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("Erro ao abrir o arquivo: %v", err)
	}
	defer file.Close()

	// Extrair o nome base do arquivo
	baseFileName := filepath.Base(filePath)

	// Enviar o comando de upload e o nome base do arquivo
	fmt.Fprintf(conn, "UPLOAD %s\n", baseFileName)

	// Enviar o arquivo
	_, err = io.Copy(conn, file)
	if err != nil {
		return fmt.Errorf("Erro ao enviar o arquivo: %v", err)
	}

	fmt.Printf("Arquivo '%s' enviado para o servidor.\n", baseFileName)
	return nil
}

func downloadFile(conn net.Conn, fileName string) error {
	// Enviar o comando de download
	fmt.Fprintf(conn, "DOWNLOAD %s\n", fileName)

	// Ler a resposta do servidor
	reader := bufio.NewReader(conn)
	response, err := reader.ReadString('\n')
	if err != nil {
		return fmt.Errorf("Erro ao ler a resposta do servidor: %v", err)
	}

	// Verificar se a resposta contém "ERROR"
	if strings.HasPrefix(response, "ERROR") {
		return fmt.Errorf(response)
	}

	// Caso não seja um erro, devemos assumir que é o tamanho do arquivo
	fileSizeStr := strings.TrimSpace(response)
	fileSize, err := strconv.ParseInt(fileSizeStr, 10, 64)
	if err != nil {
		return fmt.Errorf("Erro ao converter o tamanho do arquivo: %v", err)
	}

	// Criar o arquivo localmente para salvar o download
	file, err := os.Create(fileName)
	if err != nil {
		return fmt.Errorf("Erro ao criar o arquivo: %v", err)
	}
	defer file.Close()

	// Receber o arquivo do servidor
	receivedBytes := int64(0)
	for receivedBytes < fileSize {
		n, err := io.CopyN(file, conn, 1024)
		if err != nil && err != io.EOF {
			return fmt.Errorf("Erro ao baixar o arquivo: %v", err)
		}
		receivedBytes += n
	}

	fmt.Printf("Download do arquivo '%s' concluído.\n", fileName)
	return nil
}

func main() {
	conn, err := net.Dial("tcp", "localhost:8081")
	if err != nil {
		fmt.Println("Erro ao conectar ao super nó:", err)
		return
	}
	defer conn.Close()

	var choice int
	fmt.Println("Escolha uma opção: 1 - Upload | 2 - Download")
	fmt.Scan(&choice)

	if choice == 1 {
		fmt.Println("Digite o caminho do arquivo para upload:")
		var filePath string
		fmt.Scan(&filePath)
		err := uploadFile(conn, filePath)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Upload concluído com sucesso.")
		}
	} else if choice == 2 {
		fmt.Println("Digite o nome do arquivo para download:")
		var fileName string
		fmt.Scan(&fileName)
		err := downloadFile(conn, fileName)
		if err != nil {
			fmt.Println(err)
		} else {
			fmt.Println("Download concluído com sucesso.")
		}
	} else {
		fmt.Println("Opção inválida")
	}
}
