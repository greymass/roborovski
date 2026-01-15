package config

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestLoad_BasicTypes(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
name = test-app
count = 42
enabled = true
timeout = 5s
`), 0644)

	type Config struct {
		Name    string        `name:"name"`
		Count   int           `name:"count"`
		Enabled bool          `name:"enabled"`
		Timeout time.Duration `name:"timeout"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Name != "test-app" {
		t.Errorf("Name = %q, want %q", cfg.Name, "test-app")
	}
	if cfg.Count != 42 {
		t.Errorf("Count = %d, want %d", cfg.Count, 42)
	}
	if !cfg.Enabled {
		t.Error("Enabled = false, want true")
	}
	if cfg.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want %v", cfg.Timeout, 5*time.Second)
	}
}

func TestLoad_Defaults(t *testing.T) {
	type Config struct {
		Name    string        `default:"default-name"`
		Count   int           `default:"10"`
		Enabled bool          `default:"true"`
		Timeout time.Duration `default:"30s"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{}, &LoadOptions{SkipAutoConfig: true})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Name != "default-name" {
		t.Errorf("Name = %q, want %q", cfg.Name, "default-name")
	}
	if cfg.Count != 10 {
		t.Errorf("Count = %d, want %d", cfg.Count, 10)
	}
	if !cfg.Enabled {
		t.Error("Enabled = false, want true")
	}
	if cfg.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v, want %v", cfg.Timeout, 30*time.Second)
	}
}

func TestLoad_CLIOverridesINI(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
name = ini-value
count = 10
`), 0644)

	type Config struct {
		Name  string `name:"name"`
		Count int    `name:"count"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath, "-name", "cli-value"}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Name != "cli-value" {
		t.Errorf("Name = %q, want %q (CLI should override INI)", cfg.Name, "cli-value")
	}
	if cfg.Count != 10 {
		t.Errorf("Count = %d, want %d (INI value)", cfg.Count, 10)
	}
}

func TestLoad_INIOverridesDefault(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`count = 99`), 0644)

	type Config struct {
		Name  string `default:"default-name"`
		Count int    `default:"10"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Name != "default-name" {
		t.Errorf("Name = %q, want %q (default)", cfg.Name, "default-name")
	}
	if cfg.Count != 99 {
		t.Errorf("Count = %d, want %d (INI should override default)", cfg.Count, 99)
	}
}

func TestLoad_Required(t *testing.T) {
	type Config struct {
		Required string `required:"true"`
		Optional string
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{}, &LoadOptions{SkipAutoConfig: true})
	if err == nil {
		t.Fatal("Expected error for missing required field")
	}

	cfg = &Config{}
	err = LoadWithOptions(cfg, []string{"-required", "value"}, &LoadOptions{SkipAutoConfig: true})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Required != "value" {
		t.Errorf("Required = %q, want %q", cfg.Required, "value")
	}
}

func TestLoad_KebabCase(t *testing.T) {
	type Config struct {
		MyLongFieldName string
		HTTPAddr        string `name:"http-addr"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-my-long-field-name", "value1", "-http-addr", "value2"}, &LoadOptions{SkipAutoConfig: true})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.MyLongFieldName != "value1" {
		t.Errorf("MyLongFieldName = %q, want %q", cfg.MyLongFieldName, "value1")
	}
	if cfg.HTTPAddr != "value2" {
		t.Errorf("HTTPAddr = %q, want %q", cfg.HTTPAddr, "value2")
	}
}

func TestLoad_Aliases(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`old-name = from-alias`), 0644)

	type Config struct {
		NewName string `name:"new-name" alias:"old-name,legacy-name"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.NewName != "from-alias" {
		t.Errorf("NewName = %q, want %q", cfg.NewName, "from-alias")
	}
}

func TestLoad_StringSlice(t *testing.T) {
	type Config struct {
		Items []string `name:"items"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-items", "a,b,c"}, &LoadOptions{SkipAutoConfig: true})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	want := []string{"a", "b", "c"}
	if !reflect.DeepEqual(cfg.Items, want) {
		t.Errorf("Items = %v, want %v", cfg.Items, want)
	}
}

func TestLoad_StringSliceFromINI(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`items = one, two, three`), 0644)

	type Config struct {
		Items []string `name:"items"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	want := []string{"one", "two", "three"}
	if !reflect.DeepEqual(cfg.Items, want) {
		t.Errorf("Items = %v, want %v", cfg.Items, want)
	}
}

func TestLoad_UintTypes(t *testing.T) {
	type Config struct {
		Port  uint   `name:"port"`
		Limit uint32 `name:"limit"`
		Count int64  `name:"count"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-port", "8080", "-limit", "1000", "-count", "999999999"}, &LoadOptions{SkipAutoConfig: true})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Port != 8080 {
		t.Errorf("Port = %d, want %d", cfg.Port, 8080)
	}
	if cfg.Limit != 1000 {
		t.Errorf("Limit = %d, want %d", cfg.Limit, 1000)
	}
	if cfg.Count != 999999999 {
		t.Errorf("Count = %d, want %d", cfg.Count, 999999999)
	}
}

func TestLoad_AutoConfig(t *testing.T) {
	dir := t.TempDir()
	origWd, _ := os.Getwd()
	os.Chdir(dir)
	defer os.Chdir(origWd)

	os.WriteFile(filepath.Join(dir, "config.ini"), []byte(`name = auto-loaded`), 0644)

	type Config struct {
		Name string `name:"name"`
	}

	cfg := &Config{}
	err := Load(cfg, []string{})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Name != "auto-loaded" {
		t.Errorf("Name = %q, want %q (should auto-load ./config.ini)", cfg.Name, "auto-loaded")
	}
}

func TestLoad_StrictINI(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
name = test
unknown-key = value
`), 0644)

	type Config struct {
		Name string `name:"name"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
		StrictINI:      true,
	})
	if err == nil {
		t.Fatal("Expected error for unknown INI key in strict mode")
	}

	cfg = &Config{}
	err = LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
		StrictINI:      false,
	})
	if err != nil {
		t.Fatalf("Load should succeed in non-strict mode: %v", err)
	}
}

func TestLoad_NonPointer(t *testing.T) {
	type Config struct {
		Name string
	}

	err := Load(Config{}, []string{})
	if err == nil {
		t.Fatal("Expected error for non-pointer config")
	}
}

func TestLoad_NonStruct(t *testing.T) {
	var s string
	err := Load(&s, []string{})
	if err == nil {
		t.Fatal("Expected error for non-struct config")
	}
}

func TestLoad_InvalidDefault(t *testing.T) {
	type Config struct {
		Count int `default:"not-a-number"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{}, &LoadOptions{SkipAutoConfig: true})
	if err == nil {
		t.Fatal("Expected error for invalid default value")
	}
}

func TestLoad_QuotedINIValues(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
double-quoted = "value with spaces"
single-quoted = 'another value'
`), 0644)

	type Config struct {
		DoubleQuoted string `name:"double-quoted"`
		SingleQuoted string `name:"single-quoted"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.DoubleQuoted != "value with spaces" {
		t.Errorf("DoubleQuoted = %q, want %q", cfg.DoubleQuoted, "value with spaces")
	}
	if cfg.SingleQuoted != "another value" {
		t.Errorf("SingleQuoted = %q, want %q", cfg.SingleQuoted, "another value")
	}
}

func TestLoad_InvalidINIFormat(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`invalid line without equals`), 0644)

	type Config struct {
		Name string `name:"name"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err == nil {
		t.Fatal("Expected error for invalid INI format")
	}
}

func TestLoad_MissingConfigFile(t *testing.T) {
	type Config struct {
		Name string `name:"name"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", "/nonexistent/config.ini"}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err == nil {
		t.Fatal("Expected error for missing config file")
	}
}

func TestLoad_UnexportedFields(t *testing.T) {
	type Config struct {
		Exported   string
		unexported string
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-exported", "value"}, &LoadOptions{SkipAutoConfig: true})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.Exported != "value" {
		t.Errorf("Exported = %q, want %q", cfg.Exported, "value")
	}
}

func TestToKebabCase(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"Name", "name"},
		{"HTTPAddr", "h-t-t-p-addr"},
		{"MyLongFieldName", "my-long-field-name"},
		{"GOGC", "g-o-g-c"},
		{"Simple", "simple"},
		{"camelCase", "camel-case"},
	}

	for _, tc := range tests {
		got := toKebabCase(tc.input)
		if got != tc.want {
			t.Errorf("toKebabCase(%q) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

func TestIsZeroValue(t *testing.T) {
	tests := []struct {
		name string
		val  interface{}
		want bool
	}{
		{"empty string", "", true},
		{"non-empty string", "hello", false},
		{"zero int", int(0), true},
		{"non-zero int", int(42), false},
		{"zero int64", int64(0), true},
		{"non-zero int64", int64(42), false},
		{"zero uint", uint(0), true},
		{"non-zero uint", uint(42), false},
		{"zero uint32", uint32(0), true},
		{"non-zero uint32", uint32(42), false},
		{"false bool", false, true},
		{"true bool", true, false},
		{"empty slice", []string{}, true},
		{"non-empty slice", []string{"a"}, false},
	}

	for _, tc := range tests {
		v := reflect.ValueOf(tc.val)
		got := isZeroValue(v)
		if got != tc.want {
			t.Errorf("isZeroValue(%v) = %v, want %v", tc.val, got, tc.want)
		}
	}
}

func TestLoad_BoolValues(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
yes-value = yes
on-value = on
one-value = 1
true-value = TRUE
`), 0644)

	type Config struct {
		YesValue  bool `name:"yes-value"`
		OnValue   bool `name:"on-value"`
		OneValue  bool `name:"one-value"`
		TrueValue bool `name:"true-value"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if !cfg.YesValue {
		t.Error("YesValue should be true for 'yes'")
	}
	if !cfg.OnValue {
		t.Error("OnValue should be true for 'on'")
	}
	if !cfg.OneValue {
		t.Error("OneValue should be true for '1'")
	}
	if !cfg.TrueValue {
		t.Error("TrueValue should be true for 'TRUE'")
	}
}

func TestLoad_InvalidTypeConversion(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`count = not-a-number`), 0644)

	type Config struct {
		Count int `name:"count"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err == nil {
		t.Fatal("Expected error for invalid int conversion")
	}
}

func TestLoad_InvalidDuration(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`timeout = invalid`), 0644)

	type Config struct {
		Timeout time.Duration `name:"timeout"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err == nil {
		t.Fatal("Expected error for invalid duration")
	}
}

func TestLoad_InvalidUint(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`port = -1`), 0644)

	type Config struct {
		Port uint `name:"port"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err == nil {
		t.Fatal("Expected error for negative uint")
	}
}

func TestLoad_InvalidUint32(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`limit = invalid`), 0644)

	type Config struct {
		Limit uint32 `name:"limit"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err == nil {
		t.Fatal("Expected error for invalid uint32")
	}
}

func TestLoad_InvalidInt64(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`count = invalid`), 0644)

	type Config struct {
		Count int64 `name:"count"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err == nil {
		t.Fatal("Expected error for invalid int64")
	}
}

func TestLoad_MultipleRequired(t *testing.T) {
	type Config struct {
		First  string `required:"true"`
		Second string `required:"true"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{}, &LoadOptions{SkipAutoConfig: true})
	if err == nil {
		t.Fatal("Expected error for missing required fields")
	}
}

func TestLoad_UintFromINI(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
port = 8080
limit = 1000
`), 0644)

	type Config struct {
		Port  uint   `name:"port"`
		Limit uint32 `name:"limit"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Port != 8080 {
		t.Errorf("Port = %d, want %d", cfg.Port, 8080)
	}
	if cfg.Limit != 1000 {
		t.Errorf("Limit = %d, want %d", cfg.Limit, 1000)
	}
}

func TestLoad_Int64FromINI(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`count = 9999999999`), 0644)

	type Config struct {
		Count int64 `name:"count"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Count != 9999999999 {
		t.Errorf("Count = %d, want %d", cfg.Count, int64(9999999999))
	}
}

func TestLoad_DurationFromCLI(t *testing.T) {
	type Config struct {
		Timeout time.Duration `name:"timeout"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-timeout", "2m30s"}, &LoadOptions{SkipAutoConfig: true})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	want := 2*time.Minute + 30*time.Second
	if cfg.Timeout != want {
		t.Errorf("Timeout = %v, want %v", cfg.Timeout, want)
	}
}

func TestLoad_EmptyStringSlice(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`items = `), 0644)

	type Config struct {
		Items []string `name:"items"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Items != nil && len(cfg.Items) != 0 {
		t.Errorf("Items = %v, want empty/nil", cfg.Items)
	}
}

func TestSetFieldValue_UnsupportedType(t *testing.T) {
	type unsupported struct {
		Field float64
	}

	v := reflect.ValueOf(&unsupported{}).Elem().Field(0)
	err := setFieldValue(v, v.Type(), "1.5")
	if err == nil {
		t.Fatal("Expected error for unsupported type")
	}
}

func TestLoad_SingleSection(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
name = main-app
debug = true

[database]
host = localhost
port = 5432
`), 0644)

	type DatabaseConfig struct {
		Host string `name:"host"`
		Port int    `name:"port"`
	}

	type Config struct {
		Name     string         `name:"name"`
		Debug    bool           `name:"debug"`
		Database DatabaseConfig `section:"database"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Name != "main-app" {
		t.Errorf("Name = %q, want %q", cfg.Name, "main-app")
	}
	if !cfg.Debug {
		t.Error("Debug = false, want true")
	}
	if cfg.Database.Host != "localhost" {
		t.Errorf("Database.Host = %q, want %q", cfg.Database.Host, "localhost")
	}
	if cfg.Database.Port != 5432 {
		t.Errorf("Database.Port = %d, want %d", cfg.Database.Port, 5432)
	}
}

func TestLoad_MultipleSections(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
listen = :8080

[route]
path = /api
backend = http://api:8000

[route.2]
path = /web
backend = http://web:3000
timeout = 60
`), 0644)

	type RouteConfig struct {
		Path    string `name:"path"`
		Backend string `name:"backend"`
		Timeout int    `name:"timeout" default:"30"`
	}

	type Config struct {
		Listen string        `name:"listen"`
		Routes []RouteConfig `sections:"route"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Listen != ":8080" {
		t.Errorf("Listen = %q, want %q", cfg.Listen, ":8080")
	}
	if len(cfg.Routes) != 2 {
		t.Fatalf("len(Routes) = %d, want 2", len(cfg.Routes))
	}
	if cfg.Routes[0].Path != "/api" {
		t.Errorf("Routes[0].Path = %q, want %q", cfg.Routes[0].Path, "/api")
	}
	if cfg.Routes[0].Backend != "http://api:8000" {
		t.Errorf("Routes[0].Backend = %q, want %q", cfg.Routes[0].Backend, "http://api:8000")
	}
	if cfg.Routes[1].Path != "/web" {
		t.Errorf("Routes[1].Path = %q, want %q", cfg.Routes[1].Path, "/web")
	}
	if cfg.Routes[1].Timeout != 60 {
		t.Errorf("Routes[1].Timeout = %d, want %d", cfg.Routes[1].Timeout, 60)
	}
}

func TestLoad_SectionWithAliases(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
[actionindex]
address = localhost:9000
`), 0644)

	type ActionIndexConfig struct {
		Address string `name:"address" alias:"addr,host"`
	}

	type Config struct {
		ActionIndex ActionIndexConfig `section:"actionindex"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.ActionIndex.Address != "localhost:9000" {
		t.Errorf("ActionIndex.Address = %q, want %q", cfg.ActionIndex.Address, "localhost:9000")
	}
}

func TestLoad_UnknownSectionIgnored(t *testing.T) {
	dir := t.TempDir()
	iniPath := filepath.Join(dir, "config.ini")
	os.WriteFile(iniPath, []byte(`
name = test

[unknown]
foo = bar

name = should-not-override
`), 0644)

	type Config struct {
		Name string `name:"name"`
	}

	cfg := &Config{}
	err := LoadWithOptions(cfg, []string{"-config", iniPath}, &LoadOptions{
		ConfigFlag:     "config",
		SkipAutoConfig: true,
	})
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if cfg.Name != "should-not-override" {
		t.Errorf("Name = %q, want %q", cfg.Name, "should-not-override")
	}
}
