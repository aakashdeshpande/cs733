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
	files, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment1/files", nil)
	expiry, _ := leveldb.OpenFile("$GOPATH/src/github.com/aakashdeshpande/cs733/assignment1/expiryTimes", nil)
	defer db.Close()
	defer files.Close()
	defer expiry.Close()

	var mutex = &sync.RWMutex{}

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
		go handleConnection(conn, db, files, expiry, mutex)
	}

}

func write(conn net.Conn, fileSize int64, remainder int64, bufferRead int, buf []byte, sLargePointer *string) []byte {

	file := make([]byte, fileSize)
	// Total bytes of file written
	var currentByte int64 = 0

	// If we have already read part of the file along with the command
	if remainder > 0 {
		bufSplit := buf[int64(bufferRead)-remainder : bufferRead]
		if int64(len(bufSplit)) > fileSize {
			*sLargePointer = string(buf[int64(bufferRead)-remainder+fileSize : bufferRead])
			bufSplit = bufSplit[:fileSize]
		}
		for i := 0; i < len(bufSplit); i++ {
			file[int64(i)+currentByte] = bufSplit[i]
		}
		//_, err := file.WriteAt(bufSplit, currentByte)
		currentByte += remainder
	}

	var n int = 0
	// Keep reading connection until file bytes are read
	for {
		if currentByte >= fileSize {
			break
		}
		buf = make([]byte, 1024)

		n, _ = conn.Read(buf)
		if int64(n) > fileSize-currentByte {
			bufSplit := buf[:fileSize-currentByte]
			for i := 0; i < len(bufSplit); i++ {
				file[int64(i)+currentByte] = bufSplit[i]
			}
			//_, err = file.WriteAt(bufSplit, currentByte)
			*sLargePointer = string(buf[fileSize-currentByte : n]) // Residual string after file is read
		} else {
			for i := 0; i < n; i++ {
				file[int64(i)+currentByte] = buf[i]
			}
			//_, err = file.WriteAt(buf, currentByte)
		}
		currentByte += int64(n)
	}
	return file
}

func handleConnection(conn net.Conn, db *leveldb.DB, files *leveldb.DB, expiry *leveldb.DB, mutex *sync.RWMutex) {

	defer conn.Close() // make sure to close the connection even if we panic.
	// Make a buffer to hold incoming data with size bufferSize
	var bufferSize int64 = 1024
	buf := make([]byte, bufferSize)
	var fileName string
	var sLarge string = "" // Initial command string and number of bytes read with the command
	var bufferRead int = 0

	// Iterate over different commands, takes residual string as input
OUTER:
	for {

		// Check residual string from previous command
		buf = []byte(sLarge)
		bufferRead = len([]byte(sLarge))

		// Read the incoming connection into the buffer
		// Warning : We might not get the complete message
		for {
			if strings.Contains(sLarge, "\r\n") || len(sLarge) > 100000 {
				break
			}
			buf = make([]byte, bufferSize)
			n, _ := conn.Read(buf)
			sLarge += string(buf[:n]) // Read till we get the complete command
			bufferRead = n
		}

		//fmt.Println(sLarge, len(sLarge))
		s := strings.Split(sLarge, "\r\n") // Isolate main command from input
		commands := strings.Split(s[0], " ")
		// Remove extra input apart from basic command string
		var remainder int64 = int64(len([]byte(sLarge)) - len([]byte(s[0]+"\r\n")))
		sLarge = sLarge[len(s[0]+"\r\n"):]

		if len(commands) < 2 {
			conn.Write([]byte("ERR_CMD_ERR\r\n"))
			continue OUTER
		} else {
			fileName = commands[1]
		}

		if commands[0] == "read" {
			mutex.RLock()
			file, err := files.Get([]byte(fileName), nil)
			if err != nil {
				conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
				mutex.RUnlock()
				continue OUTER
			}
			//Check expiry
			var diff int64 = 0
			expbytes, err := expiry.Get([]byte(fileName), nil)
			if err == nil {
				exp, _ := strconv.ParseInt(string(expbytes), 10, 64)
				if exp != -1 {
					diff = exp - time.Now().Unix()
				}
			}
			if diff < 0 {
				conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
				mutex.RUnlock()
				continue OUTER
			}

			// Check version
			var version int64 = 0
			data, err := db.Get([]byte(fileName), nil)
			if err == nil {
				version, err = strconv.ParseInt(string(data), 10, 64)
			}
			mutex.RUnlock()

			conn.Write([]byte("CONTENTS " + strconv.FormatInt(version, 10) + " " + strconv.Itoa(len(file)-2) + " " + strconv.FormatInt(diff, 10) + "\r\n"))

			for i := 0; i < len(file)-2; i++ {
				_, err = conn.Write(file[i : i+1])
			}
			_, err = conn.Write([]byte("\r\n"))

			if err != nil && err != io.EOF {
				conn.Write([]byte("ERR_INTERNAL\r\n"))
			}
		} else if commands[0] == "delete" {
			err := files.Delete([]byte(fileName), nil) // Remove entry from database
			//err := os.Remove(fileName)
			if err != nil {
				conn.Write([]byte("ERR_FILE_NOT_FOUND\r\n"))
			} else {
				db.Delete([]byte(fileName), nil)     // Remove entry from database
				expiry.Delete([]byte(fileName), nil) // Remove entry from database
				conn.Write([]byte("OK\r\n"))
			}
		} else if commands[0] == "write" {
			sLarge = ""

			if len(commands) < 3 {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
				continue OUTER
			}

			// NUmber of bytes to be written to the file
			fileSize, err := strconv.ParseInt(commands[2], 10, 64)
			fileSize = fileSize + int64(len([]byte("\r\n")))

			file := write(conn, fileSize, remainder, bufferRead, buf, &sLarge)

			mutex.Lock()
			err = files.Put([]byte(fileName), file, nil)

			// Get file version
			var version int64 = 0
			data, err := db.Get([]byte(fileName), nil)
			if err == nil {
				version, err = strconv.ParseInt(string(data), 10, 64)
			}

			err = db.Put([]byte(fileName), []byte(strconv.FormatInt(version+1, 10)), nil)
			conn.Write([]byte("OK " + strconv.FormatInt(version+1, 10) + "\r\n"))

			// Write expiry time
			if len(commands) > 3 {
				exp, _ := strconv.ParseInt(commands[3], 10, 64)
				if exp == 0 {
					err = expiry.Put([]byte(fileName), []byte(strconv.FormatInt(-1, 10)), nil)
				} else {
					err = expiry.Put([]byte(fileName), []byte(strconv.FormatInt(time.Now().Unix()+exp, 10)), nil)
				}
			}
			mutex.Unlock()

		} else if commands[0] == "cas" {
			sLarge = ""

			if len(commands) < 4 {
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

			if err != nil {
				conn.Write([]byte("ERR_INTERNAL\r\n"))
				mutex.Unlock()
				return
			}

			fileVersion, err := strconv.ParseInt(commands[2], 10, 64)

			// Check for version match
			if version != fileVersion {
				conn.Write([]byte("ERR_VERSION\r\n"))
				mutex.Unlock()
				continue OUTER
			}

			// Number of bytes to be written to file
			fileSize, err := strconv.ParseInt(commands[3], 10, 64)
			fileSize = fileSize + int64(len([]byte("\r\n")))

			file := write(conn, fileSize, remainder, bufferRead, buf, &sLarge)

			err = files.Put([]byte(fileName), file, nil)

			err = db.Put([]byte(fileName), []byte(strconv.FormatInt(version, 10)), nil)
			conn.Write([]byte("OK " + strconv.FormatInt(version, 10) + "\r\n"))

			// Write expiry time
			if len(commands) > 4 {
				exp, _ := strconv.ParseInt(commands[4], 10, 64)
				if exp == 0 {
					err = expiry.Put([]byte(fileName), []byte(strconv.FormatInt(-1, 10)), nil)
				} else {
					err = expiry.Put([]byte(fileName), []byte(strconv.FormatInt(time.Now().Unix()+exp, 10)), nil)
				}
			}
			mutex.Unlock()

		} else {
			conn.Write([]byte("ERR_CMD_ERR\r\n"))
			continue OUTER
		}

	}
}

func main() {
	serverMain()
}
