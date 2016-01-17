package main

import "fmt"
import "net"
import "io"
import "os"
import "strings"
import "strconv"
import "github.com/syndtr/goleveldb/leveldb"
import "sync"
import "time"
//import "reflect"
//import "io/ioutil"

func serverMain() {
    // Database to store file version   
    db, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment1/versionMap", nil)
    defer db.Close()

    var mutex = &sync.Mutex{}
  
	// Listen for TCP connection on port 8080
    ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		fmt.Println("Error listening:", err.Error())
        os.Exit(1)
	}
  
    // Keep listening for new connections
	for {
		conn, err := ln.Accept()
		if err != nil {
			fmt.Println("Error accepting: ", err.Error())
            os.Exit(1)
		}
        // Handle new connection in a new thread
		go handleConnection(conn, db, mutex)
	}
  
}

func handleConnection(conn net.Conn, db *leveldb.DB, mutex *sync.Mutex) {
    
    defer conn.Close() // make sure to close the connection even if we panic.
    // Make a buffer to hold incoming data with size bufferSize
    var bufferSize int64 = 1024
    buf := make([]byte, bufferSize)
    var fileName string 
    
    // Initial command string and number of bytes read with the command
    var sLarge string = ""
    var bufferRead int = 0

    // Iterate over different commands, takes residual string as input
    OUTER:    
    for{

        // Check residual string from previous command
        buf = []byte(sLarge)   
        bufferRead = len([]byte(sLarge)) 

        // Read the incoming connection into the buffer
        // Warning : We might not get the complete message
        for{
            if strings.Contains(sLarge, "\r\n") || len(sLarge) > 100000{
                break
            }
            buf = make([]byte, bufferSize)
            n, err := conn.Read(buf)
            if err != nil {
                if err != io.EOF {
                    fmt.Println("Error reading:", err.Error())
                }
            }
            sLarge += string(buf[:n]) // Read till we get the complete command
            bufferRead = n
        }
        
        s := strings.Split(sLarge, "\r\n") // Isolate main command from input 
        commands := strings.Split(s[0], " ")
        // Remove extra input apart from basic command string

        var remainder int64 = int64(len([]byte(sLarge)) - len([]byte(s[0] + "\r\n")))
        sLarge = sLarge[len(s[0] + "\r\n"):]

        if(len(commands) < 2){
            conn.Write([]byte("ERR_CMD_ERR\r\n"))
            continue OUTER
        }else { 
            fileName = commands[1]
        }

        if commands[0] == "read" {
            file, err := os.Open(fileName) // For read access.
            if err != nil {
                conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
                return
            }
            defer file.Close() // make sure to close the file even if we panic.

            //_, err = io.Copy(conn, file)

            var currentByte int64 = 0
            fileBuffer := make([]byte, bufferSize)

            // Read file until there is an error
            for {
                n, err := file.ReadAt(fileBuffer, currentByte)
                currentByte += int64(n)
                //fmt.Println(fileBuffer) //This is just for checking
                conn.Write(fileBuffer[:n])
                if err == io.EOF {
                    break
                }
                if err != nil && err != io.EOF {
                    conn.Write([]byte("ERR_INTERNAL\r\n"))
                    break
                } 
            }

            if err != nil && err != io.EOF {
                conn.Write([]byte("ERR_INTERNAL\r\n"))
            } else { 
                // Print version
                var version int64 = 0
                data, err := db.Get([]byte(fileName), nil)
                if err == nil {
                    version, err = strconv.ParseInt(string(data), 10, 64)
                }
                conn.Write([]byte("OK " + strconv.FormatInt(version,10) + "\r\n"))
            }
        } else if commands[0] == "delete" {
            err := os.Remove(fileName)
            if err != nil {
                conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
            } else {
                db.Delete([]byte(fileName), nil) // Remove entry from database
                conn.Write([]byte("OK \r\n"))
            }
        } else if commands[0] == "write" {
            sLarge = ""

            if(len(commands) < 3){
                conn.Write([]byte("ERR_CMD_ERR\r\n"))
                continue OUTER
            }

            file, err := os.Create(fileName) // For write access or to create the file otherwise.
            if err != nil {
                conn.Write([]byte("ERR_INTERNAL\r\n"))
                fmt.Println(err)
                return
            }
            defer file.Close() // make sure to close the file even if we panic.

            // NUmber of bytes to be written to the file
            fileSize, err := strconv.ParseInt(commands[2], 10, 64)
            fileSize = fileSize + int64(len([]byte("\r\n")))

            mutex.Lock()

            // Total bytes of file written
            var currentByte int64 = 0
            // If we have already read part of the file along with the command
            if len(s) > 1{
                bufSplit := buf[int64(bufferRead) - remainder:bufferRead]
                if int64(len(bufSplit)) > fileSize {
                    sLarge = string(buf[int64(bufferRead) - remainder + fileSize:bufferRead])
                    bufSplit = bufSplit[:fileSize]
                }
                _, err := file.WriteAt(bufSplit, currentByte)
                currentByte += int64(bufferRead - len([]byte(s[0] + "\r\n")))
                if err != nil {
                    conn.Write([]byte("ERR_INTERNAL\r\n"))
                    return
                }
            }

            // Check for timeout
            c1 := make(chan bool, 1) 
            if len(commands) > 3 {
                exp, _  := strconv.Atoi(commands[3])
                if exp > 0{
                    go func(exp int) {
                        time.Sleep(time.Second * time.Duration(exp))
                        c1 <- true
                    }(exp)
                }
            }

            var res bool = false 
            var n int = 0
            // Keep reading connection until file bytes are read
            for {
                if currentByte >= fileSize {
                    break
                }
                buf = make([]byte, bufferSize)
                
                go func(n *int, conn net.Conn, buf *[]byte) {
                    *n, _ = conn.Read(*buf)
                    c1 <- false
                }(&n, conn, &buf)
                
                // Check for timeout or input
                res = <- c1
                if res == true {
                    break
                }

                if int64(n) > fileSize - currentByte {
                    bufSplit := buf[:fileSize - currentByte]
                    _, err = file.WriteAt(bufSplit, currentByte)
                    sLarge = string(buf[fileSize - currentByte:]) // Residual string after file is read
                } else {
                    _, err = file.WriteAt(buf, currentByte)
                }
                if err != nil {
                    conn.Write([]byte("ERR_INTERNAL\r\n"))
                    return
                }
                currentByte += int64(n)    
            }

            // Get file version
            var version int64 = 0
            data, err := db.Get([]byte(fileName), nil)
            if err == nil {
                version, err = strconv.ParseInt(string(data), 10, 64)
            }

            err = db.Put([]byte(fileName), []byte(strconv.FormatInt(version+1, 10)), nil)
            conn.Write([]byte("OK " + strconv.FormatInt(version+1, 10) + "\r\n"))    
            mutex.Unlock()

            // If timeout, clear residual string
            if res == true {
                <- c1
                sLarge = string(buf[:n])
            }
        } else if commands[0] == "cas" {
            sLarge = ""

            if(len(commands) < 4){
                conn.Write([]byte("ERR_CMD_ERR\r\n"))
                continue OUTER
            } 

            mutex.Lock()

            // Get file version
            var version int64 = 0
            data, err := db.Get([]byte(fileName), nil)
            if err == nil {
                version, err = strconv.ParseInt(string(data), 10, 64)
            }

            fileVersion, err := strconv.ParseInt(commands[2], 10, 64)

            if err != nil {
                conn.Write([]byte("ERR_INTERNAL\r\n"))
                fmt.Println(err)
                return
            }

            // Check for version match
            if version != fileVersion {
                conn.Write([]byte("ERR_VERSION\r\n"))
                fmt.Println(err)
                mutex.Unlock()
                continue OUTER
            }        

            file, err := os.Create(fileName)
            defer file.Close() // make sure to close the file even if we panic.
            if err != nil {
                conn.Write([]byte("ERR_INTERNAL\r\n"))
                fmt.Println(err)
                return
            }

            // Number of bytes to be written to file
            fileSize, err := strconv.ParseInt(commands[3], 10, 64)
            fileSize = fileSize + int64(len([]byte("\r\n")))

            var currentByte int64 = 0
            // If we have already read part of the file along with the command
            if len(s) > 1{
                bufSplit := buf[int64(bufferRead) - remainder:bufferRead]
                if int64(len(bufSplit)) > fileSize {
                    sLarge = string(buf[int64(bufferRead) - remainder + fileSize:bufferRead])
                    bufSplit = bufSplit[:fileSize]
                }
                _, err := file.WriteAt(bufSplit, currentByte)
                currentByte += int64(bufferRead - len([]byte(s[0] + "\r\n")))
                if err != nil {
                    conn.Write([]byte("ERR_INTERNAL\r\n"))
                    return
                }
            }

            // Check for timeout
            c1 := make(chan bool, 1) 
            if len(commands) > 4 {
                exp, _  := strconv.Atoi(commands[4])
                if exp > 0{
                    go func(exp int) {
                        time.Sleep(time.Second * time.Duration(exp))
                        c1 <- true
                    }(exp)
                }
            }

            var res bool = false 
            var n int = 0
            // Keep reading connection until file bytes are read
            for {
                if currentByte >= fileSize {
                    break
                }
                buf = make([]byte, bufferSize)
                
                go func(n *int, conn net.Conn, buf *[]byte) {
                    *n, _ = conn.Read(*buf)
                    c1 <- false
                }(&n, conn, &buf)
                
                // Check for timeout or input
                res = <- c1
                if res == true {
                    break
                }

                if int64(n) > fileSize - currentByte {
                    bufSplit := buf[:fileSize - currentByte]
                    _, err = file.WriteAt(bufSplit, currentByte)
                    sLarge = string(buf[fileSize - currentByte:]) // Residual string after file is read
                } else {
                    _, err = file.WriteAt(buf, currentByte)
                }
                if err != nil {
                    conn.Write([]byte("ERR_INTERNAL\r\n"))
                    return
                }
                currentByte += int64(n)    
            }

            file.Sync() // Check
            err = db.Put([]byte(fileName), []byte(strconv.FormatInt(version, 10)), nil)
            conn.Write([]byte("OK " + strconv.FormatInt(version, 10) + "\r\n"))
            mutex.Unlock()

            // If timeout, clear residual string
            if res == true {
                <- c1
                sLarge = string(buf[:n])
            }
        } else {
            conn.Write([]byte("ERR_CMD_ERR\r\n"))
            continue OUTER
        }

    }
}


func main() {
	serverMain()
}
