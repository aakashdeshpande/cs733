package main

import "fmt"
import "net"
import "io"
//import "io/ioutil"
import "os"
import "strings"
import "strconv"
import "github.com/syndtr/goleveldb/leveldb"
//import "reflect"

func serverMain() {
  //versionMap := map[string]int{}
  db, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment1/versionMap", nil)
  defer db.Close()
  
	ln, err := net.Listen("tcp", ":8080")
	if err != nil {
		// handle error
		fmt.Println("Error listening:", err.Error())
        os.Exit(1)
	}
  
	for {
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			fmt.Println("Error accepting: ", err.Error())
            os.Exit(1)
		}
		go handleConnection(conn, db)//, versionMap)
	}
  
}

func handleConnection(conn net.Conn, db *leveldb.DB) {//, versionMap map[string]int) {
  
    defer conn.Close()
    // Make a buffer to hold incoming data.
    var bufferSize int64 = 32
    buf := make([]byte, bufferSize)
    //var terminated bool
    var fileName string 

    // Read the incoming connection into the buffer.
    // Warning : We might not get the complete message
    //for {
        n, err := conn.Read(buf)
        // Was there an error in reading ?
        if err != nil {
            if err != io.EOF {
                fmt.Println("Error reading:", err.Error())
            }
            //break
        }

        sLarge := string(buf[:n])
        s := strings.Split(sLarge, "\r\n")
        commands := strings.Split(s[0], " ")
        /*
        if strings.Count(sLarge, "\r\n") == 1 {
            terminated = false
        } else if strings.Count(sLarge, "\r\n") == 2 {
            terminated = true
        }
        */
    //}
    
    if(len(commands) < 2){
        conn.Write([]byte("ERR_CMD_ERR\r\n"))
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
        _, err = io.Copy(conn, file)
        if err != nil {
            conn.Write([]byte("ERR_INTERNAL\r\n"))
        } else {
            var version int64 = 0
            data, err := db.Get([]byte(fileName), nil)
            if err == nil {
                version, err = strconv.ParseInt(string(data), 10, 64)
            }
            conn.Write([]byte("OK " + strconv.FormatInt(version+1,10) + "\r\n"))
        }
    } else if commands[0] == "delete" {
        err := os.Remove(fileName)
        if err != nil {
            conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
        } else {
            err := db.Delete([]byte(fileName), nil)
            if err != nil {
                conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
            } else {
                conn.Write([]byte("OK \r\n"))
            }
        }
    } else if commands[0] == "write" {
        var version int64 = 0
        _, err := os.Stat(fileName); 
        data, err := db.Get([]byte(fileName), nil)
        if err == nil {
            version, err = strconv.ParseInt(string(data), 10, 64)
        }
        /*
        if os.IsNotExist(err) {
            // path/to/whatever does not exist
            //version := 0
            versionMap[fileName] = 0
        } else {
            _, err := versionMap[fileName]
            if err == false {
                versionMap[fileName] = 0
            }
        }
        */

        file, err := os.Create(fileName)
        defer file.Close() // make sure to close the file even if we panic.
        if err != nil {
            conn.Write([]byte("ERR_INTERNAL\r\n"))
            fmt.Println(err)
            return
        }

        fileSize, err := strconv.ParseInt(commands[2], 10, 64)

        var currentByte int64 = 0
        if len(s) > 1{
            bufSplit := buf[len([]byte(s[0] + "\r\n")):n]
            if int64(len(bufSplit)) > fileSize {
                bufSplit = bufSplit[:fileSize]
            }
            _, err := file.WriteAt(bufSplit, currentByte)
            currentByte += int64(len(bufSplit))
            if err != nil {
                conn.Write([]byte("ERR_INTERNAL\r\n"))
                return
            }
        }

        //var lastElement string = ""
        //if terminated == false {
        for {
            if currentByte >= int64(fileSize) {
                break
            }
            n, err := conn.Read(buf)
            if int64(n) > fileSize - currentByte {
                buf = buf[:fileSize - currentByte]
            }
            _, err = file.WriteAt(buf, currentByte)
            currentByte += int64(n)
            if err != nil {
                conn.Write([]byte("ERR_INTERNAL\r\n"))
                return
            }
            //fileString := string(buf[:n])
            //if lastElement + fileString[0:1] == "\r\n" || strings.Count(fileString, "\r\n") > 0 { 
            //    break
            //}
            //lastElement = fileString[len(fileString) - 1:]
        }
        //}

        file.Sync()
        //versionMap[fileName]++
        err = db.Put([]byte(fileName), []byte(strconv.FormatInt(version+1, 10)), nil)
        conn.Write([]byte("OK " + strconv.FormatInt(version+1, 10) + "\r\n"))
    } else if commands[0] == "cas" {
        d2 := []byte(s[1])
        f, _ := os.Create("/Users/Aakash/Documents/" + fileName)
        f.Write(d2)
        f.Sync()
    } else {
        conn.Write([]byte("ERR_CMD_ERR\r\n"))
    }

}


func main() {
	serverMain()
	fmt.Printf("Hello, world.\n")
}
