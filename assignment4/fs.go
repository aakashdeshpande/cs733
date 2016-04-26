package main

//import "strings"
//import "fmt"
import "strconv"
import "github.com/syndtr/goleveldb/leveldb"
import "sync"
import "time"
import "encoding/json"


type FileService struct{
	Id	int64
	db 		*leveldb.DB 
	files 	*leveldb.DB
	expiry 	*leveldb.DB
	mutex 	*sync.RWMutex
	input	<-chan CommitInfo
	routes	[]chan Response
}

func NewFS(Id int, routes []chan Response, input <-chan CommitInfo) *FileService{
	var f *FileService = &FileService{}
	f.Id = int64(Id)
	f.routes = routes
	f.input = input
	// Database to store file version
	f.db, _ = leveldb.OpenFile("versionMap" + strconv.Itoa(Id), nil)
	f.files, _ = leveldb.OpenFile("files" + strconv.Itoa(Id), nil)
	f.expiry, _ = leveldb.OpenFile("expiryTimes" + strconv.Itoa(Id), nil)
	f.mutex = &sync.RWMutex{}
	return f
}

func (f *FileService) Listen() {
	defer f.db.Close()
	defer f.files.Close()
	defer f.expiry.Close()

	for { 
		ci := <- f.input
		var n Msg
		if(ci.Err != nil) {
			break
		}
		json.Unmarshal(ci.Data, &n)
		f.ProcessMsg(n, ci.Leader)
		//fmt.Println("Heard ", f.Id, " ", n.Kind, ";", n.Filename, ":")
	}
}

func (f *FileService) ProcessMsg(msg Msg, toSend bool) {
	switch msg.Kind {
	case 0:
		// fmt.Println(msg.Filename, " ", toSend)
		f.mutex.RLock()
		file, err := f.files.Get([]byte(msg.Filename), nil)
		if err != nil {
			f.mutex.RUnlock()

			if (toSend) {f.routes[msg.Id] <- Response{"ERR_FILE_NOT_FOUND\r\n", true, nil}}
			return
		}
		//Check expiry
		var diff int64 = 0
		expbytes, err := f.expiry.Get([]byte(msg.Filename), nil)
		if err == nil {
			exp, _ := strconv.ParseInt(string(expbytes), 10, 64)
			if exp != -1 {
				diff = exp - time.Now().Unix()
			}
		}
		if diff < 0 {
			f.mutex.RUnlock()
			if (toSend) {f.routes[msg.Id] <- Response{"ERR_FILE_NOT_FOUND\r\n", true, nil}}
			return
		}

		// Check version
		var version int64 = 0
		data, err := f.db.Get([]byte(msg.Filename), nil)
		if err == nil {
			version, err = strconv.ParseInt(string(data), 10, 64)
		}
		f.mutex.RUnlock()	

		if (toSend) {f.routes[msg.Id] <- Response{"CONTENTS " + strconv.FormatInt(version, 10) + " " + strconv.Itoa(len(file)-2) + " " + strconv.FormatInt(diff, 10) + "\r\n", false, file}}
		return

	case 1:
		err := f.files.Delete([]byte(msg.Filename), nil) // Remove entry from database
		//err := os.Remove(msg.Filename)
		if err != nil {
			if (toSend) {f.routes[msg.Id] <- Response{"ERR_FILE_NOT_FOUND\r\n", true, nil}}
			return
		} else {
			f.db.Delete([]byte(msg.Filename), nil)     // Remove entry from database
			f.expiry.Delete([]byte(msg.Filename), nil) // Remove entry from database
			if (toSend) {f.routes[msg.Id] <- Response{"OK\r\n", false, nil}}
			return
		}	

	case 2:
		f.mutex.Lock()

		f.files.Put([]byte(msg.Filename), msg.Contents, nil)
		// Get file version
		var version int64 = 0
		data, err := f.db.Get([]byte(msg.Filename), nil)
		if err == nil {
			version, err = strconv.ParseInt(string(data), 10, 64)
		}

		err = f.db.Put([]byte(msg.Filename), []byte(strconv.FormatInt(version+1, 10)), nil)

		if msg.Exptime == 0 {
			err = f.expiry.Put([]byte(msg.Filename), []byte(strconv.FormatInt(-1, 10)), nil)
		} else {
			err = f.expiry.Put([]byte(msg.Filename), []byte(strconv.FormatInt(time.Now().Unix()+msg.Exptime, 10)), nil)
		}

		f.mutex.Unlock()
		if (toSend) {f.routes[msg.Id] <- Response{"OK " + strconv.FormatInt(version+1, 10) + "\r\n", false, nil}}
		return

	case 3:
		f.mutex.Lock()

		// Get file version
		var version int64 = 0
		data, err := f.db.Get([]byte(msg.Filename), nil)
		if err == nil {
			version, err = strconv.ParseInt(string(data), 10, 64)
		}

		if err != nil {
			f.mutex.Unlock()
			if (toSend) {f.routes[msg.Id] <- Response{"ERR_INTERNAL\r\n", true, nil}}
			return
		}

		// Check for version match
		if version != msg.Version {
			f.mutex.Unlock()
			if (toSend) {f.routes[msg.Id] <- Response{"ERR_VERSION " + strconv.FormatInt(version, 10) + "\r\n", true, nil}}
			return
		}

		err = f.files.Put([]byte(msg.Filename), msg.Contents, nil)

		err = f.db.Put([]byte(msg.Filename), []byte(strconv.FormatInt(version, 10)), nil)

		if msg.Exptime == 0 {
			err = f.expiry.Put([]byte(msg.Filename), []byte(strconv.FormatInt(-1, 10)), nil)
		} else {
			err = f.expiry.Put([]byte(msg.Filename), []byte(strconv.FormatInt(time.Now().Unix()+msg.Exptime, 10)), nil)
		}

		f.mutex.Unlock()	
		if (toSend) {f.routes[msg.Id] <- Response{"OK " + strconv.FormatInt(version, 10) + "\r\n", false, nil}}
		return
	}
}
