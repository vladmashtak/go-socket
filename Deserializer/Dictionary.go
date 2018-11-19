package Deserializer

type Dictionary map[string]interface{}

func NewDictionary(size uint32) Dictionary {
	return make(map[string]interface{}, size)
}
