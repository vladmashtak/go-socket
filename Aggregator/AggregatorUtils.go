package Aggregator

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
)

func parseValueToLong(value interface{}) int64 {
	if result, ok := value.(uint64); !ok {
		return 0
	} else {
		return int64(result)
	}
}

func parseValueToInt(value interface{}) int32 {
	if result, ok := value.(uint64); !ok {
		return 0
	} else {
		return int32(result)
	}
}

func parseValueToShort(value interface{}) uint16 {
	if result, ok := value.(uint16); !ok {
		return 0
	} else {
		return result
	}
}

func parseValueToVlan(value interface{}) uint16 {
	if result, ok := value.(uint16); !ok {
		return 32767
	} else {
		return result
	}
}

func parseValueToIpv6(value interface{}) string {
	var (
		netIp net.IP
		ok    bool
	)

	if netIp, ok = value.([]byte); !ok {
		return ""
	} else {
		return netIp.To16().String()
	}
}

func parseValueToIpv4(value interface{}) string {
	if ip, ok := value.(uint64); !ok {
		return ""
	} else {
		netIp := make(net.IP, 4)
		binary.BigEndian.PutUint32(netIp, uint32(ip))

		return netIp.String()
	}
}

func parseValueToString(value interface{}) string {
	if result, ok := value.(string); !ok {
		return ""
	} else {
		return result
	}
}

func parseValueToArrayByte(value interface{}) []byte {
	if result, ok := value.([]byte); !ok {
		return make([]byte, 0)
	} else {
		return result
	}
}

func createKeyValuePairs(m map[string]interface{}) string {
	b := new(bytes.Buffer)
	for key, value := range m {
		fmt.Fprintf(b, "%s=\"%v\"\n", key, value)
	}
	return b.String()
}
