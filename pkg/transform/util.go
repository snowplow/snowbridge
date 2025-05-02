package transform

func RemoveNullFields(data any) {
	switch input := data.(type) {
	case map[string]any:
		removeNullFromMap(input)
	case []any:
		removeNullFromSlice(input)
	default:
		return
	}
}

func removeNullFromMap(input map[string]any) {
	for key := range input {
		field := input[key]
		if field == nil {
			delete(input, key)
			continue
		}
		RemoveNullFields(field)
	}
}

func removeNullFromSlice(input []any) {
	for _, item := range input {
		RemoveNullFields(item)
	}
}
