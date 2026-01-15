package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestINIParser_String(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`name = test-value`), 0644)

	var name string
	err := NewINIParser(iniPath).String("name", &name).ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if name != "test-value" {
		t.Errorf("name = %q, want %q", name, "test-value")
	}
}

func TestINIParser_Int(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`count = 42`), 0644)

	var count int
	err := NewINIParser(iniPath).Int("count", &count).ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if count != 42 {
		t.Errorf("count = %d, want %d", count, 42)
	}
}

func TestINIParser_Int_Invalid(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`count = not-a-number`), 0644)

	var count int
	err := NewINIParser(iniPath).Int("count", &count).ParseWithUnknownHandler(nil)
	if err == nil {
		t.Fatal("Expected error for invalid int")
	}
}

func TestINIParser_Bool(t *testing.T) {
	tests := []struct {
		value string
		want  bool
	}{
		{"true", true},
		{"TRUE", true},
		{"True", true},
		{"yes", true},
		{"YES", true},
		{"1", true},
		{"on", true},
		{"ON", true},
		{"false", false},
		{"no", false},
		{"0", false},
		{"off", false},
		{"anything-else", false},
	}

	for _, tc := range tests {
		t.Run(tc.value, func(t *testing.T) {
			dir := t.TempDir()
			iniPath := filepath.Join(dir, "config.ini")
			os.WriteFile(iniPath, []byte("enabled = "+tc.value), 0644)

			var enabled bool
			err := NewINIParser(iniPath).Bool("enabled", &enabled).ParseWithUnknownHandler(nil)
			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			if enabled != tc.want {
				t.Errorf("enabled = %v, want %v for input %q", enabled, tc.want, tc.value)
			}
		})
	}
}

func TestINIParser_Duration(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`timeout = 30s`), 0644)

	var timeout time.Duration
	err := NewINIParser(iniPath).Duration("timeout", &timeout).ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if timeout != 30*time.Second {
		t.Errorf("timeout = %v, want %v", timeout, 30*time.Second)
	}
}

func TestINIParser_Duration_Complex(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`timeout = 2h30m15s`), 0644)

	var timeout time.Duration
	err := NewINIParser(iniPath).Duration("timeout", &timeout).ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	want := 2*time.Hour + 30*time.Minute + 15*time.Second
	if timeout != want {
		t.Errorf("timeout = %v, want %v", timeout, want)
	}
}

func TestINIParser_Duration_Invalid(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`timeout = invalid`), 0644)

	var timeout time.Duration
	err := NewINIParser(iniPath).Duration("timeout", &timeout).ParseWithUnknownHandler(nil)
	if err == nil {
		t.Fatal("Expected error for invalid duration")
	}
}

func TestINIParser_StringSlice(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`items = a, b, c`), 0644)

	var items []string
	err := NewINIParser(iniPath).StringSlice("items", &items, ",").ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(items, want) {
		t.Errorf("items = %v, want %v", items, want)
	}
}

func TestINIParser_StringSlice_Empty(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`items = `), 0644)

	items := []string{"existing"}
	err := NewINIParser(iniPath).StringSlice("items", &items, ",").ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if items != nil {
		t.Errorf("items = %v, want nil for empty", items)
	}
}

func TestINIParser_StringSlice_CustomSeparator(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`items = a|b|c`), 0644)

	var items []string
	err := NewINIParser(iniPath).StringSlice("items", &items, "|").ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(items, want) {
		t.Errorf("items = %v, want %v", items, want)
	}
}

func TestINIParser_Alias(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`old-name = aliased-value`), 0644)

	var name string
	err := NewINIParser(iniPath).
		String("new-name", &name).
		Alias("old-name", "new-name").
		ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if name != "aliased-value" {
		t.Errorf("name = %q, want %q", name, "aliased-value")
	}
}

func TestINIParser_Parse_IgnoresUnknown(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
known = value
unknown = other
`), 0644)

	var known string
	err := NewINIParser(iniPath).String("known", &known).ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if known != "value" {
		t.Errorf("known = %q, want %q", known, "value")
	}
}

func TestINIParser_ParseStrict_RejectsUnknown(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
known = value
unknown = other
`), 0644)

	var known string
	err := NewINIParser(iniPath).String("known", &known).ParseStrict()
	if err == nil {
		t.Fatal("Expected error for unknown key in strict mode")
	}
}

func TestINIParser_ParseWithUnknownHandler(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
known = value
extra1 = foo
extra2 = bar
`), 0644)

	var known string
	unknownKeys := make(map[string]string)
	err := NewINIParser(iniPath).
		String("known", &known).
		ParseWithUnknownHandler(func(key, value string, lineNum int) error {
			unknownKeys[key] = value
			return nil
		})
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if known != "value" {
		t.Errorf("known = %q, want %q", known, "value")
	}
	if unknownKeys["extra1"] != "foo" || unknownKeys["extra2"] != "bar" {
		t.Errorf("unknownKeys = %v, expected extra1=foo, extra2=bar", unknownKeys)
	}
}

func TestINIParser_Comments(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
# This is a comment
name = value
# Another comment
count = 42
`), 0644)

	var name string
	var count int
	err := NewINIParser(iniPath).
		String("name", &name).
		Int("count", &count).
		ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if name != "value" {
		t.Errorf("name = %q, want %q", name, "value")
	}
	if count != 42 {
		t.Errorf("count = %d, want %d", count, 42)
	}
}

func TestINIParser_EmptyLines(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
name = value

count = 42

`), 0644)

	var name string
	var count int
	err := NewINIParser(iniPath).
		String("name", &name).
		Int("count", &count).
		ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if name != "value" {
		t.Errorf("name = %q, want %q", name, "value")
	}
	if count != 42 {
		t.Errorf("count = %d, want %d", count, 42)
	}
}

func TestINIParser_QuotedValues(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
double = "value with spaces"
single = 'another value'
`), 0644)

	var double, single string
	err := NewINIParser(iniPath).
		String("double", &double).
		String("single", &single).
		ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if double != "value with spaces" {
		t.Errorf("double = %q, want %q", double, "value with spaces")
	}
	if single != "another value" {
		t.Errorf("single = %q, want %q", single, "another value")
	}
}

func TestINIParser_InvalidFormat(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`invalid line`), 0644)

	var name string
	err := NewINIParser(iniPath).String("name", &name).ParseWithUnknownHandler(nil)
	if err == nil {
		t.Fatal("Expected error for invalid format")
	}
}

func TestINIParser_MissingFile(t *testing.T) {
	err := NewINIParser("/nonexistent/config.ini").ParseWithUnknownHandler(nil)
	if err == nil {
		t.Fatal("Expected error for missing file")
	}
}

func TestINIParser_MultipleHandlersChained(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
name = test
count = 42
enabled = true
timeout = 5s
items = a,b,c
`), 0644)

	var name string
	var count int
	var enabled bool
	var timeout time.Duration
	var items []string

	err := NewINIParser(iniPath).
		String("name", &name).
		Int("count", &count).
		Bool("enabled", &enabled).
		Duration("timeout", &timeout).
		StringSlice("items", &items, ",").
		ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if name != "test" {
		t.Errorf("name = %q, want %q", name, "test")
	}
	if count != 42 {
		t.Errorf("count = %d, want %d", count, 42)
	}
	if !enabled {
		t.Error("enabled = false, want true")
	}
	if timeout != 5*time.Second {
		t.Errorf("timeout = %v, want %v", timeout, 5*time.Second)
	}
	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(items, want) {
		t.Errorf("items = %v, want %v", items, want)
	}
}

func TestParseBool(t *testing.T) {
	trueValues := []string{"true", "TRUE", "True", "yes", "YES", "Yes", "1", "on", "ON", "On"}
	for _, v := range trueValues {
		if !ParseBool(v) {
			t.Errorf("ParseBool(%q) = false, want true", v)
		}
	}

	falseValues := []string{"false", "FALSE", "no", "NO", "0", "off", "OFF", "", "anything"}
	for _, v := range falseValues {
		if ParseBool(v) {
			t.Errorf("ParseBool(%q) = true, want false", v)
		}
	}
}

func TestINIParser_WhitespaceHandling(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
  name  =  value with leading/trailing spaces  
	tabbed	=	value
`), 0644)

	var name, tabbed string
	err := NewINIParser(iniPath).
		String("name", &name).
		String("tabbed", &tabbed).
		ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if name != "value with leading/trailing spaces" {
		t.Errorf("name = %q, want %q", name, "value with leading/trailing spaces")
	}
	if tabbed != "value" {
		t.Errorf("tabbed = %q, want %q", tabbed, "value")
	}
}

func TestINIParser_ValuesWithEquals(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`url = http://example.com?foo=bar&baz=qux`), 0644)

	var url string
	err := NewINIParser(iniPath).String("url", &url).ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	if url != "http://example.com?foo=bar&baz=qux" {
		t.Errorf("url = %q, want %q", url, "http://example.com?foo=bar&baz=qux")
	}
}

func TestINIParser_StringSlice_WithWhitespace(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`items = , a ,  , b , c , `), 0644)

	var items []string
	err := NewINIParser(iniPath).StringSlice("items", &items, ",").ParseWithUnknownHandler(nil)
	if err != nil {
		t.Fatalf("Parse failed: %v", err)
	}

	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(items, want) {
		t.Errorf("items = %v, want %v (should trim empty entries)", items, want)
	}
}
