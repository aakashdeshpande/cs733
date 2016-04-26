package main

//import "fmt"
import "net"
import "io"
import "strings"
import "strconv"
import "encoding/json"

func Replicate(msg []byte, rafts []RaftNode) {
	var ldr *RaftNode
	for {
		ldr = getLeader(rafts)
		if (ldr != nil) { 
			break
		}
	}
	//fmt.Println("Leader chosen to Replicate ", ldr.Id())
	ldr.Append(msg)
}

// Read fileSize bytes and write to file
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
			*sLargePointer = string(buf[fileSize-currentByte : n]) // Residual string after file is read
		} else {
			for i := 0; i < n; i++ {
				file[int64(i)+currentByte] = buf[i]
			}
		}
		currentByte += int64(n)
	}
	return file
}

type ClientHandler struct{
	Id	int64
	route chan Response
}

func (c *ClientHandler) handleConnection(conn net.Conn, rafts []RaftNode) {

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
			msg := Msg{c.Id, 0, fileName, nil, -1, -1}
			b, _ := json.Marshal(msg)
			Replicate(b, rafts)
			//fmt.Println("Reached almost :P")
			r := <- c.route
			//fmt.Println("Reached almost :P")
			if r.Err {
				conn.Write([]byte(r.Output))
				continue OUTER
			} else {
				conn.Write([]byte(r.Output))
			}

			for i := 0; i < len(r.File)-2; i++ {
				conn.Write(r.File[i : i+1])
			}
			_, err := conn.Write([]byte("\r\n"))

			if err != nil && err != io.EOF {
				conn.Write([]byte("ERR_INTERNAL\r\n"))
			}
		} else if commands[0] == "delete" {
			msg := Msg{c.Id, 1, fileName, nil, -1, -1}
			b, _ := json.Marshal(msg)
			Replicate(b, rafts)
			r := <- c.route
			conn.Write([]byte(r.Output))
		} else if commands[0] == "write" {
			sLarge = ""

			if len(commands) < 3 {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
				continue OUTER
			}

			// NUmber of bytes to be written to the file
			fileSize, err := strconv.ParseInt(commands[2], 10, 64)
			if err != nil {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
				continue OUTER
			}
			fileSize = fileSize + int64(len([]byte("\r\n")))

			file := write(conn, fileSize, remainder, bufferRead, buf, &sLarge)

			// Write expiry time
			var exp int64  = 0
			if len(commands) > 3 {
				exp, _ = strconv.ParseInt(commands[3], 10, 64)
			}

			msg := Msg{c.Id, 2, fileName, file, exp, -1}
			b, _ := json.Marshal(msg)
			Replicate(b, rafts)
			r := <- c.route
			conn.Write([]byte(r.Output))

		} else if commands[0] == "cas" {
			sLarge = ""

			if len(commands) < 4 {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
				continue OUTER
			}

			// Number of bytes to be written to file
			fileSize, err := strconv.ParseInt(commands[3], 10, 64)
			if err != nil {
				conn.Write([]byte("ERR_CMD_ERR\r\n"))
				continue OUTER
			}
			fileSize = fileSize + int64(len([]byte("\r\n")))

			file := write(conn, fileSize, remainder, bufferRead, buf, &sLarge)

			fileVersion, err := strconv.ParseInt(commands[2], 10, 64)

			// Write f.expiry time
			var exp int64  = 0
			if len(commands) > 4 {
				exp, _ = strconv.ParseInt(commands[4], 10, 64)
			}

			msg := Msg{c.Id, 3, fileName, file, exp, fileVersion}
			b, _ := json.Marshal(msg)
			Replicate(b, rafts)
			r := <- c.route
			conn.Write([]byte(r.Output))
			

		} else {
			conn.Write([]byte("ERR_CMD_ERR\r\n"))
			continue OUTER
		}

	}
}