package catalog

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
)

func canonicalJSON(value any) ([]byte, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("marshal canonical value: %w", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var decoded any
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("decode canonical value: %w", err)
	}
	var output bytes.Buffer
	if err := writeCanonicalJSON(&output, decoded); err != nil {
		return nil, err
	}
	return output.Bytes(), nil
}

func canonicalSHA256(value any) (string, []byte, error) {
	encoded, err := canonicalJSON(value)
	if err != nil {
		return "", nil, err
	}
	encoded = append(append([]byte(nil), encoded...), '\n')
	digest := sha256.Sum256(encoded)
	return fmt.Sprintf("sha256:%x", digest), encoded, nil
}

func writeCanonicalJSON(output *bytes.Buffer, value any) error {
	switch value := value.(type) {
	case nil:
		return writeCanonicalString(output, "null")
	case bool:
		if value {
			return writeCanonicalString(output, "true")
		}
		return writeCanonicalString(output, "false")
	case string:
		encoded, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("encode canonical string: %w", err)
		}
		return writeCanonicalBytes(output, encoded)
	case json.Number:
		if _, err := strconv.ParseFloat(string(value), 64); err != nil {
			return fmt.Errorf("encode canonical number %q: %w", value, err)
		}
		return writeCanonicalString(output, string(value))
	case []any:
		if err := writeCanonicalByte(output, '['); err != nil {
			return err
		}
		for index, item := range value {
			if index > 0 {
				if err := writeCanonicalByte(output, ','); err != nil {
					return err
				}
			}
			if err := writeCanonicalJSON(output, item); err != nil {
				return err
			}
		}
		return writeCanonicalByte(output, ']')
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		if err := writeCanonicalByte(output, '{'); err != nil {
			return err
		}
		for index, key := range keys {
			if index > 0 {
				if err := writeCanonicalByte(output, ','); err != nil {
					return err
				}
			}
			if err := writeCanonicalJSON(output, key); err != nil {
				return err
			}
			if err := writeCanonicalByte(output, ':'); err != nil {
				return err
			}
			if err := writeCanonicalJSON(output, value[key]); err != nil {
				return err
			}
		}
		return writeCanonicalByte(output, '}')
	default:
		return fmt.Errorf("encode canonical value of type %T", value)
	}
}

func writeCanonicalString(output *bytes.Buffer, value string) error {
	if _, err := output.WriteString(value); err != nil {
		return fmt.Errorf("write canonical JSON string: %w", err)
	}
	return nil
}

func writeCanonicalBytes(output *bytes.Buffer, value []byte) error {
	if _, err := output.Write(value); err != nil {
		return fmt.Errorf("write canonical JSON bytes: %w", err)
	}
	return nil
}

func writeCanonicalByte(output *bytes.Buffer, value byte) error {
	if err := output.WriteByte(value); err != nil {
		return fmt.Errorf("write canonical JSON byte: %w", err)
	}
	return nil
}
