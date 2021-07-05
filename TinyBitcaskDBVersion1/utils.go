package TinyBitcaskDB

import "log"

const Debug = 1

func DPrintf(format string, args ... interface{}){
	if Debug > 0{
		log.Printf(format, args ... )
	}
}

