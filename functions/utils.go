package functions

import (
	"fmt"
	"os"
	"reflect"
)

func GetAppSetting(setting string, defaultValue string) string {
	if appSettingValue, exists := os.LookupEnv(setting); exists {
		return appSettingValue
	}

	return defaultValue
}

func GetFunctionDetails(f interface{}) error {
	fType := reflect.TypeOf(f)
	if fType.Kind() != reflect.Func {
		return fmt.Errorf("expected a function, got %s", fType.Kind())
	}

	fmt.Printf("Name: %s\n", reflect.TypeOf(f).String())
	fmt.Printf("Number of Parameters: %d\n", fType.NumIn())
	fmt.Printf("Number of Return Values: %d\n", fType.NumOut())
	fmt.Printf("Kind: %s\n", fType.Kind().String())

	// Print parameter types
	for i := 0; i < fType.NumIn(); i++ {
		fmt.Printf("Parameter %d Type: %s\n", i+1, fType.In(i))
	}

	// Print return types
	for i := 0; i < fType.NumOut(); i++ {
		fmt.Printf("Return %d Type: %s\n", i+1, fType.Out(i))
	}

	inputs := make([]reflect.Value, fType.NumIn())
	for i := 0; i < fType.NumIn(); i++ {
		argType := fType.In(i)

		// Provide zero values for each parameter
		inputs[i] = reflect.Zero(argType)
		fmt.Printf("Providing default value for parameter %d: %v (type: %s)\n", i+1, inputs[i], argType)
	}

	results := reflect.ValueOf(f).Call(inputs)
	for i, res := range results {
		fmt.Printf("Return %d: %v (type: %s)\n", i+1, res.Interface(), fType.Out(i))
	}

	return nil
}
