package config

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func CheckVersion(version string) {
	for _, arg := range os.Args[1:] {
		if arg == "--version" || arg == "-version" {
			fmt.Println(version)
			os.Exit(0)
		}
	}
}

type fieldInfo struct {
	field        reflect.Value
	name         string
	aliases      []string
	help         string
	fieldType    reflect.Type
	isRequired   bool
	defaultValue string
	section      string
	sections     string
}

func Load(cfg interface{}, args []string) error {
	return LoadWithOptions(cfg, args, nil)
}

type LoadOptions struct {
	ConfigFlag     string
	DefaultConfig  string
	StrictINI      bool
	SkipAutoConfig bool
}

func LoadWithOptions(cfg interface{}, args []string, opts *LoadOptions) error {
	if opts == nil {
		opts = &LoadOptions{
			ConfigFlag:    "config",
			DefaultConfig: "./config.ini",
		}
	}

	v := reflect.ValueOf(cfg)
	if v.Kind() != reflect.Ptr || v.Elem().Kind() != reflect.Struct {
		return fmt.Errorf("cfg must be a pointer to a struct")
	}
	v = v.Elem()
	t := v.Type()

	fields := parseStructTags(v, t)

	if err := applyDefaults(fields); err != nil {
		return fmt.Errorf("failed to apply defaults: %w", err)
	}

	fs := flag.NewFlagSet("config", flag.ContinueOnError)

	var configPath string
	fs.StringVar(&configPath, opts.ConfigFlag, "", "Path to config file")

	flagValues := make(map[string]interface{})
	for _, f := range fields {
		registerFlag(fs, f, flagValues)
	}

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		return err
	}

	if !opts.SkipAutoConfig {
		if configPath == "" {
			if _, err := os.Stat(opts.DefaultConfig); err == nil {
				configPath = opts.DefaultConfig
			}
		}
	}

	if configPath != "" {
		if err := loadINI(configPath, fields, opts.StrictINI); err != nil {
			return fmt.Errorf("failed to load config file: %w", err)
		}
	}

	applyFlags(fields, flagValues, fs)

	if err := validateRequired(fields); err != nil {
		return err
	}

	return nil
}

func parseStructTags(v reflect.Value, t reflect.Type) []fieldInfo {
	var fields []fieldInfo

	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		fv := v.Field(i)

		if !fv.CanSet() {
			continue
		}

		name := sf.Tag.Get("name")
		if name == "" {
			name = toKebabCase(sf.Name)
		}

		var aliases []string
		if aliasTag := sf.Tag.Get("alias"); aliasTag != "" {
			for _, a := range strings.Split(aliasTag, ",") {
				aliases = append(aliases, strings.TrimSpace(a))
			}
		}

		help := sf.Tag.Get("help")
		required := sf.Tag.Get("required") == "true"
		defaultValue := sf.Tag.Get("default")
		section := sf.Tag.Get("section")
		sections := sf.Tag.Get("sections")

		fields = append(fields, fieldInfo{
			field:        fv,
			name:         name,
			aliases:      aliases,
			help:         help,
			fieldType:    sf.Type,
			isRequired:   required,
			defaultValue: defaultValue,
			section:      section,
			sections:     sections,
		})
	}

	return fields
}

func registerFlag(fs *flag.FlagSet, f fieldInfo, values map[string]interface{}) {
	switch f.fieldType.Kind() {
	case reflect.String:
		ptr := new(string)
		fs.StringVar(ptr, f.name, "", f.help)
		values[f.name] = ptr
	case reflect.Int:
		ptr := new(int)
		fs.IntVar(ptr, f.name, 0, f.help)
		values[f.name] = ptr
	case reflect.Int64:
		if f.fieldType == reflect.TypeOf(time.Duration(0)) {
			ptr := new(time.Duration)
			fs.DurationVar(ptr, f.name, 0, f.help)
			values[f.name] = ptr
		} else {
			ptr := new(int64)
			fs.Int64Var(ptr, f.name, 0, f.help)
			values[f.name] = ptr
		}
	case reflect.Uint:
		ptr := new(uint)
		fs.UintVar(ptr, f.name, 0, f.help)
		values[f.name] = ptr
	case reflect.Uint32:
		ptr := new(uint)
		fs.UintVar(ptr, f.name, 0, f.help)
		values[f.name] = ptr
	case reflect.Bool:
		ptr := new(bool)
		fs.BoolVar(ptr, f.name, false, f.help)
		values[f.name] = ptr
	case reflect.Slice:
		if f.fieldType.Elem().Kind() == reflect.String {
			ptr := new(string)
			help := f.help
			if !strings.Contains(strings.ToLower(help), "comma") {
				help += " (comma-separated)"
			}
			fs.StringVar(ptr, f.name, "", help)
			values[f.name] = ptr
		}
	}
}

func loadINI(path string, fields []fieldInfo, strict bool) error {
	iniMap := make(map[string]*fieldInfo)
	sectionFields := make(map[string]*fieldInfo)
	sectionsFields := make(map[string]*fieldInfo)

	for i := range fields {
		f := &fields[i]
		if f.section != "" {
			sectionFields[f.section] = f
		} else if f.sections != "" {
			sectionsFields[f.sections] = f
		} else {
			iniMap[f.name] = f
			for _, alias := range f.aliases {
				iniMap[alias] = f
			}
		}
	}

	handler := func(key, value string, lineNum int) error {
		if strict {
			return fmt.Errorf("unknown configuration key at line %d: %s", lineNum, key)
		}
		return nil
	}

	return parseINIFileWithSections(path, iniMap, sectionFields, sectionsFields, handler)
}

func parseINIFileWithSections(path string, fields map[string]*fieldInfo, sectionFields map[string]*fieldInfo, sectionsFields map[string]*fieldInfo, unknownHandler func(key, value string, lineNum int) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0
	var currentSectionTarget reflect.Value
	var currentSectionFieldMap map[string]int

	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())

		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		if strings.HasPrefix(line, "[") && strings.HasSuffix(line, "]") {
			sectionName := strings.ToLower(strings.Trim(line, "[]"))

			if sf, ok := sectionFields[sectionName]; ok {
				currentSectionTarget = sf.field
				currentSectionFieldMap = buildFieldMap(sf.fieldType)
			} else {
				matched := false
				for prefix, sf := range sectionsFields {
					if sectionName == prefix || strings.HasPrefix(sectionName, prefix+".") {
						matched = true
						elemType := sf.fieldType.Elem()
						newElem := reflect.New(elemType).Elem()
						sf.field.Set(reflect.Append(sf.field, newElem))
						currentSectionTarget = sf.field.Index(sf.field.Len() - 1)
						currentSectionFieldMap = buildFieldMap(elemType)
						break
					}
				}
				if !matched {
					currentSectionTarget = reflect.Value{}
					currentSectionFieldMap = nil
				}
			}
			continue
		}

		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid format at line %d: %s", lineNum, line)
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])
		value = strings.Trim(value, `"'`)

		if currentSectionTarget.IsValid() && currentSectionFieldMap != nil {
			kebabKey := toKebabCase(key)
			if fieldIdx, ok := currentSectionFieldMap[kebabKey]; ok {
				field := currentSectionTarget.Field(fieldIdx)
				if err := setFieldValue(field, field.Type(), value); err != nil {
					return fmt.Errorf("error parsing '%s' at line %d: %w", key, lineNum, err)
				}
				continue
			}
			if fieldIdx, ok := currentSectionFieldMap[key]; ok {
				field := currentSectionTarget.Field(fieldIdx)
				if err := setFieldValue(field, field.Type(), value); err != nil {
					return fmt.Errorf("error parsing '%s' at line %d: %w", key, lineNum, err)
				}
				continue
			}
		}

		f, ok := fields[key]
		if !ok {
			if unknownHandler != nil {
				if err := unknownHandler(key, value, lineNum); err != nil {
					return err
				}
			}
			continue
		}

		if err := setFieldValue(f.field, f.fieldType, value); err != nil {
			return fmt.Errorf("error parsing '%s' at line %d: %w", key, lineNum, err)
		}
	}

	return scanner.Err()
}

func buildFieldMap(t reflect.Type) map[string]int {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	m := make(map[string]int)
	for i := 0; i < t.NumField(); i++ {
		sf := t.Field(i)
		name := sf.Tag.Get("name")
		if name == "" {
			name = toKebabCase(sf.Name)
		}
		m[name] = i
		if alias := sf.Tag.Get("alias"); alias != "" {
			for _, a := range strings.Split(alias, ",") {
				m[strings.TrimSpace(a)] = i
			}
		}
	}
	return m
}

func parseINIFile(path string, fields map[string]*fieldInfo, unknownHandler func(key, value string, lineNum int) error) error {
	file, err := os.Open(path)
	if err != nil {
		return err
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

		f, ok := fields[key]
		if !ok {
			if unknownHandler != nil {
				if err := unknownHandler(key, value, lineNum); err != nil {
					return err
				}
			}
			continue
		}

		if err := setFieldValue(f.field, f.fieldType, value); err != nil {
			return fmt.Errorf("error parsing '%s' at line %d: %w", key, lineNum, err)
		}
	}

	return scanner.Err()
}

func setFieldValue(fv reflect.Value, ft reflect.Type, value string) error {
	switch ft.Kind() {
	case reflect.String:
		fv.SetString(value)
	case reflect.Int:
		v, err := strconv.Atoi(value)
		if err != nil {
			return err
		}
		fv.SetInt(int64(v))
	case reflect.Int64:
		if ft == reflect.TypeOf(time.Duration(0)) {
			d, err := time.ParseDuration(value)
			if err != nil {
				return err
			}
			fv.Set(reflect.ValueOf(d))
		} else {
			v, err := strconv.ParseInt(value, 10, 64)
			if err != nil {
				return err
			}
			fv.SetInt(v)
		}
	case reflect.Uint:
		v, err := strconv.ParseUint(value, 10, 64)
		if err != nil {
			return err
		}
		fv.SetUint(v)
	case reflect.Uint32:
		v, err := strconv.ParseUint(value, 10, 32)
		if err != nil {
			return err
		}
		fv.SetUint(v)
	case reflect.Bool:
		fv.SetBool(ParseBool(value))
	case reflect.Slice:
		if ft.Elem().Kind() == reflect.String {
			var slice []string
			for _, item := range strings.Split(value, ",") {
				trimmed := strings.TrimSpace(item)
				if trimmed != "" {
					slice = append(slice, trimmed)
				}
			}
			fv.Set(reflect.ValueOf(slice))
		}
	default:
		return fmt.Errorf("unsupported type: %v", ft.Kind())
	}
	return nil
}

func applyFlags(fields []fieldInfo, values map[string]interface{}, fs *flag.FlagSet) {
	for _, f := range fields {
		ptr, ok := values[f.name]
		if !ok {
			continue
		}

		flagObj := fs.Lookup(f.name)
		if flagObj == nil {
			continue
		}

		visited := false
		fs.Visit(func(fl *flag.Flag) {
			if fl.Name == f.name {
				visited = true
			}
		})
		if !visited {
			continue
		}

		switch v := ptr.(type) {
		case *string:
			if *v != "" {
				if f.fieldType.Kind() == reflect.Slice && f.fieldType.Elem().Kind() == reflect.String {
					var slice []string
					for _, item := range strings.Split(*v, ",") {
						trimmed := strings.TrimSpace(item)
						if trimmed != "" {
							slice = append(slice, trimmed)
						}
					}
					f.field.Set(reflect.ValueOf(slice))
				} else {
					f.field.SetString(*v)
				}
			}
		case *int:
			f.field.SetInt(int64(*v))
		case *int64:
			f.field.SetInt(*v)
		case *uint:
			if f.fieldType.Kind() == reflect.Uint32 {
				f.field.SetUint(uint64(*v))
			} else {
				f.field.SetUint(uint64(*v))
			}
		case *bool:
			f.field.SetBool(*v)
		case *time.Duration:
			f.field.Set(reflect.ValueOf(*v))
		}
	}
}

func toKebabCase(s string) string {
	var result strings.Builder
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result.WriteByte('-')
		}
		if r >= 'A' && r <= 'Z' {
			result.WriteRune(r + 32)
		} else {
			result.WriteRune(r)
		}
	}
	return result.String()
}

func validateRequired(fields []fieldInfo) error {
	var missing []string
	for _, f := range fields {
		if !f.isRequired {
			continue
		}
		if isZeroValue(f.field) {
			missing = append(missing, f.name)
		}
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing required config: %s", strings.Join(missing, ", "))
	}
	return nil
}

func applyDefaults(fields []fieldInfo) error {
	for _, f := range fields {
		if f.defaultValue == "" {
			continue
		}
		if err := setFieldValue(f.field, f.fieldType, f.defaultValue); err != nil {
			return fmt.Errorf("invalid default for %s: %w", f.name, err)
		}
	}
	return nil
}

func isZeroValue(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.String:
		return v.String() == ""
	case reflect.Int, reflect.Int64:
		return v.Int() == 0
	case reflect.Uint, reflect.Uint32:
		return v.Uint() == 0
	case reflect.Bool:
		return !v.Bool()
	case reflect.Slice:
		return v.Len() == 0
	default:
		return v.IsZero()
	}
}
