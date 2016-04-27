package main

import "github.com/syndtr/goleveldb/leveldb"
import "fmt"
import "strconv"

func main(){
	for i:=0; i<5; i++ {
		fmt.Println("*********File System ", i, "*********")
		files, _ := leveldb.OpenFile("../files" + strconv.Itoa(i), nil)
		iter := files.NewIterator(nil, nil)
		for iter.Next() {
			// Remember that the contents of the returned slice should not be modified, and
			// only valid until the next call to Next.
			key := iter.Key()
			fmt.Println(string(key))
			value := iter.Value()
			fmt.Println(string(value))
		}
		iter.Release()		
	}
}
