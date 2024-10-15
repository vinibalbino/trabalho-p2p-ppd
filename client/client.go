// client.go
package main

import (
    "fmt"
    "net"
    "os"
    "io"
)

func uploadFile(conn net.Conn, filePath string) error {
    file, err := os.Open(filePath)
    if err != nil {
        return fmt.Errorf("Erro ao abrir o arquivo: %v", err)
    }
    defer file.Close()

    // Enviar comando de upload e o nome do arquivo
    fmt.Fprintf(conn, "UPLOAD %s\n", filePath)

    // Enviar o arquivo
    _, err = io.Copy(conn, file)
    if err != nil {
        return fmt.Errorf("Erro ao enviar o arquivo: %v", err)
    }

    return nil
}

func downloadFile(conn net.Conn, fileName string) error {
    // Enviar comando de download
    fmt.Fprintf(conn, "DOWNLOAD %s\n", fileName)

    // Criar arquivo localmente para salvar o download
    file, err := os.Create(fileName)
    if err != nil {
        return fmt.Errorf("Erro ao criar o arquivo: %v", err)
    }
    defer file.Close()

    // Receber arquivo
    _, err = io.Copy(file, conn)
    if err != nil {
        return fmt.Errorf("Erro ao baixar o arquivo: %v", err)
    }

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