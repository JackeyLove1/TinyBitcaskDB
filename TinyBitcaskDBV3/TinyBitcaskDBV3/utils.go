package TinyBitcaskDBV3

import "log"

const DBug = 0

func Check(err error) error {
	if err != nil {
		return err
	}
	return nil
}

func DPrintf(format string, args ...interface{}) {
	if DBug > 0 {
		log.Printf(format, args...)
	}
}
