package reflection

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

// MapToStruc will take a map and try to add to a struc
// This is handy when we have received JSON data and want into a structure
// This is not recomended if you are working on large datasets, probably best just to use the map directly
func MapToStruc(obj interface{}, m map[string]interface{}) error {
	for name, value := range m {
		structValue := reflect.ValueOf(obj).Elem()
		structFieldValue := structValue.FieldByName(name)

		if !structFieldValue.IsValid() {
			fmt.Printf("Ignoring field: %s in obj", name)
			//continue
		}

		if !structFieldValue.CanSet() {
			fmt.Printf("Cannot set value for field: %s in obj", name)
			continue
		}

		structFieldType := structFieldValue.Type()
		val := reflect.ValueOf(value)
		if structFieldType != val.Type() {
			fmt.Printf("rovided value type didn't match obj field type for %s", name)
			continue
		}

		structFieldValue.Set(val)
	}
	return nil
}

// UnmarshalToArray is a handy function to Unmarshal a JSON array and return the splice of interfaces
// Call it as follows: rows := reflection.UnmarshalToArray(b)
func UnmarshalToArray(in []byte) ([]interface{}, error) {
	var data interface{}
	if err := json.Unmarshal(in, &data); err != nil {
		return nil, err
	}
	switch rows := data.(type) {
	case []interface{}:
		return rows, nil
	}
	return nil, errors.New("")
}
