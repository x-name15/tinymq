package helper

import (
	"encoding/json"
	"fmt"
)

func MatchJSONFilter(payload []byte, key, val string) bool {
	if key == "" {
		return true
	}
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return false
	}
	v, exists := data[key]
	if !exists {
		return false
	}

	strVal := fmt.Sprintf("%v", v)
	return strVal == val
}
