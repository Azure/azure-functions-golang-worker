package worker

import (
	"bytes"
	"encoding/json"
)

// Keep raw string and number tokens: unmarshaling into Go values would lose
// duplicate properties, escape spellings, and potentially numeric precision.
func normalizeHostConfiguration(content []byte) ([]byte, error) {
	if len(content) > maxHostConfigurationBytes {
		return nil, errHostConfigurationTooLarge
	}
	if len(content) == 0 {
		return nil, errHostConfigurationJSON
	}

	stripped := make([]byte, 0, len(content))
	for i := 0; i < len(content); {
		if content[i] == '"' {
			end, err := hostConfigurationStringEnd(content, i)
			if err != nil {
				return nil, err
			}
			stripped = append(stripped, content[i:end]...)
			i = end
		} else if i+1 < len(content) && content[i] == '/' && content[i+1] == '/' {
			i += 2
			for i < len(content) && content[i] != '\r' && content[i] != '\n' {
				// Utf8JsonReader rejects these ambiguous line separators in
				// single-line comments rather than treating them as content.
				if i+2 < len(content) && content[i] == 0xe2 && content[i+1] == 0x80 && (content[i+2] == 0xa8 || content[i+2] == 0xa9) {
					return nil, errHostConfigurationJSON
				}
				i++
			}
			stripped = append(stripped, ' ')
		} else if i+1 < len(content) && content[i] == '/' && content[i+1] == '*' {
			end := bytes.Index(content[i+2:], []byte("*/"))
			if end < 0 {
				return nil, errHostConfigurationJSON
			}
			i += end + 4
			stripped = append(stripped, ' ')
		} else {
			stripped = append(stripped, content[i])
			i++
		}
	}

	depth := 0
	var previous byte
	for i := 0; i < len(stripped); i++ {
		switch stripped[i] {
		case '"':
			end, err := hostConfigurationStringEnd(stripped, i)
			if err != nil {
				return nil, err
			}
			i = end - 1
		case '{', '[':
			depth++
			if depth > 64 {
				return nil, errHostConfigurationJSON
			}
		case '}', ']':
			depth--
		case ',':
			next := i + 1
			for next < len(stripped) && hostConfigurationJSONSpace(stripped[next]) {
				next++
			}
			// Only a comma after a value may be trailing. Do not repair
			// invalid documents such as [,], {,}, [1,,], or {"a":,}.
			if next < len(stripped) && (stripped[next] == '}' || stripped[next] == ']') &&
				previous != '[' && previous != '{' && previous != ',' && previous != ':' {
				stripped[i] = ' '
			}
		}
		if !hostConfigurationJSONSpace(stripped[i]) {
			previous = stripped[i]
		}
	}

	var output bytes.Buffer
	if err := json.Compact(&output, stripped); err != nil {
		return nil, errHostConfigurationJSON
	}
	if output.Len() > maxHostConfigurationBytes {
		return nil, errHostConfigurationTooLarge
	}
	if output.Len() == 0 || output.Bytes()[0] != '{' {
		return nil, errHostConfigurationJSON
	}
	return output.Bytes(), nil
}

func hostConfigurationStringEnd(content []byte, start int) (int, error) {
	for i := start + 1; i < len(content); i++ {
		switch content[i] {
		case '\\':
			i++
		case '"':
			return i + 1, nil
		}
	}
	return 0, errHostConfigurationJSON
}

func hostConfigurationJSONSpace(c byte) bool {
	return c == ' ' || c == '\t' || c == '\r' || c == '\n'
}
