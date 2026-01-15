package config

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type INIParser struct {
	filename string
	handlers map[string]fieldHandler
	aliases  map[string]string
}

type fieldHandler struct {
	setter func(value string) error
}

func NewINIParser(filename string) *INIParser {
	return &INIParser{
		filename: filename,
		handlers: make(map[string]fieldHandler),
		aliases:  make(map[string]string),
	}
}

func (p *INIParser) String(key string, target *string) *INIParser {
	p.handlers[key] = fieldHandler{
		setter: func(value string) error {
			*target = value
			return nil
		},
	}
	return p
}

func (p *INIParser) Int(key string, target *int) *INIParser {
	p.handlers[key] = fieldHandler{
		setter: func(value string) error {
			val, err := strconv.Atoi(value)
			if err != nil {
				return fmt.Errorf("invalid integer value: %s", value)
			}
			*target = val
			return nil
		},
	}
	return p
}

func (p *INIParser) Bool(key string, target *bool) *INIParser {
	p.handlers[key] = fieldHandler{
		setter: func(value string) error {
			*target = ParseBool(value)
			return nil
		},
	}
	return p
}

func (p *INIParser) Duration(key string, target *time.Duration) *INIParser {
	p.handlers[key] = fieldHandler{
		setter: func(value string) error {
			dur, err := time.ParseDuration(value)
			if err != nil {
				return fmt.Errorf("invalid duration value: %s", value)
			}
			*target = dur
			return nil
		},
	}
	return p
}

func (p *INIParser) StringSlice(key string, target *[]string, separator string) *INIParser {
	p.handlers[key] = fieldHandler{
		setter: func(value string) error {
			if value == "" {
				*target = nil
				return nil
			}
			*target = nil
			for _, item := range strings.Split(value, separator) {
				trimmed := strings.TrimSpace(item)
				if trimmed != "" {
					*target = append(*target, trimmed)
				}
			}
			return nil
		},
	}
	return p
}

func (p *INIParser) Alias(key string, aliasFor string) *INIParser {
	p.aliases[key] = aliasFor
	return p
}

func (p *INIParser) ParseStrict() error {
	return p.ParseWithUnknownHandler(func(key, value string, lineNum int) error {
		return fmt.Errorf("unknown configuration key at line %d: %s", lineNum, key)
	})
}

func (p *INIParser) ParseWithUnknownHandler(unknownHandler func(key, value string, lineNum int) error) error {
	file, err := os.Open(p.filename)
	if err != nil {
		return fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid format at line %d: %s", lineNum, line)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		value = strings.Trim(value, `"'`)

		resolvedKey := key
		if alias, ok := p.aliases[key]; ok {
			resolvedKey = alias
		}

		handler, ok := p.handlers[resolvedKey]
		if !ok {
			if unknownHandler != nil {
				if err := unknownHandler(key, value, lineNum); err != nil {
					return err
				}
			}
			continue
		}

		if err := handler.setter(value); err != nil {
			return fmt.Errorf("error parsing '%s' at line %d: %w", key, lineNum, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading config file: %w", err)
	}

	return nil
}

func ParseBool(value string) bool {
	value = strings.ToLower(value)
	return value == "true" || value == "yes" || value == "1" || value == "on"
}
