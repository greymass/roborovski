package openapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

const testSpecYAML = `openapi: 3.0.3
info:
  title: Test API
  version: 1.0.0
tags:
  - name: blocks
    description: Block operations
  - name: actions
    description: Action operations
  - name: unused
    description: Unused tag
paths:
  /v1/blocks:
    get:
      tags:
        - blocks
      summary: Get blocks
      responses:
        '200':
          description: Success
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/BlockList'
  /v1/blocks/{id}:
    get:
      tags:
        - blocks
      summary: Get block by ID
      parameters:
        - name: id
          in: path
          required: true
          schema:
            type: integer
      responses:
        '200':
          description: Success
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Block'
  /v1/actions:
    get:
      tags:
        - actions
      summary: Get actions
      responses:
        '200':
          description: Success
    post:
      tags:
        - actions
      summary: Create action
      requestBody:
        content:
          application/json:
            schema:
              $ref: '#/components/schemas/ActionInput'
      responses:
        '201':
          description: Created
  /v1/health:
    get:
      summary: Health check
      responses:
        '200':
          description: OK
    head:
      summary: Health check (HEAD)
      responses:
        '200':
          description: OK
  /v1/admin:
    put:
      summary: Update admin
      responses:
        '200':
          description: OK
    delete:
      summary: Delete admin
      responses:
        '204':
          description: Deleted
    patch:
      summary: Patch admin
      responses:
        '200':
          description: OK
    options:
      summary: Options
      responses:
        '200':
          description: OK
components:
  schemas:
    Block:
      type: object
      properties:
        id:
          type: integer
        hash:
          type: string
    BlockList:
      type: object
      properties:
        blocks:
          type: array
          items:
            $ref: '#/components/schemas/Block'
    ActionInput:
      type: object
      properties:
        name:
          type: string
    UnusedSchema:
      type: object
      properties:
        unused:
          type: boolean
`

func TestLoad(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	if spec == nil {
		t.Fatal("expected non-nil spec")
	}

	model := spec.Model()
	if model == nil {
		t.Fatal("expected non-nil model")
	}

	if model.Info.Title != "Test API" {
		t.Errorf("expected title 'Test API', got %q", model.Info.Title)
	}

	if model.Info.Version != "1.0.0" {
		t.Errorf("expected version '1.0.0', got %q", model.Info.Version)
	}
}

func TestLoadWithVersion(t *testing.T) {
	spec, err := LoadWithVersion([]byte(testSpecYAML), "2.0.0")
	if err != nil {
		t.Fatalf("LoadWithVersion failed: %v", err)
	}

	if spec.Model().Info.Version != "2.0.0" {
		t.Errorf("expected version '2.0.0', got %q", spec.Model().Info.Version)
	}
}

func TestLoadWithVersionEmpty(t *testing.T) {
	spec, err := LoadWithVersion([]byte(testSpecYAML), "")
	if err != nil {
		t.Fatalf("LoadWithVersion with empty version failed: %v", err)
	}

	if spec.Model().Info.Version != "1.0.0" {
		t.Errorf("expected original version '1.0.0', got %q", spec.Model().Info.Version)
	}
}

func TestLoadInvalidYAML(t *testing.T) {
	_, err := Load([]byte("not: valid: yaml: {{"))
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}

func TestLoadInvalidOpenAPI(t *testing.T) {
	invalidSpec := `openapi: 3.0.3
info:
  title: Test
paths:
  /test:
    get:
      responses:
        '200':
          $ref: '#/components/responses/NonExistent'
`
	spec, err := Load([]byte(invalidSpec))
	if err != nil {
		return
	}
	if spec != nil {
		t.Log("spec loaded but may have invalid references")
	}
}

func TestYAML(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	yaml := spec.YAML()
	if len(yaml) == 0 {
		t.Error("expected non-empty YAML output")
	}

	if !strings.Contains(string(yaml), "Test API") {
		t.Error("YAML should contain API title")
	}

	if !strings.Contains(string(yaml), "openapi:") {
		t.Error("YAML should contain openapi key")
	}
}

func TestJSON(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	json := spec.JSON()
	if len(json) == 0 {
		t.Error("expected non-empty JSON output")
	}

	if !strings.Contains(string(json), "Test API") {
		t.Error("JSON should contain API title")
	}

	if !strings.Contains(string(json), `"openapi"`) {
		t.Error("JSON should contain openapi key")
	}
}

func TestPaths(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	paths := spec.Paths()
	if len(paths) != 5 {
		t.Errorf("expected 5 paths, got %d: %v", len(paths), paths)
	}

	expectedPaths := map[string]bool{
		"/v1/blocks":      true,
		"/v1/blocks/{id}": true,
		"/v1/actions":     true,
		"/v1/health":      true,
		"/v1/admin":       true,
	}

	for _, path := range paths {
		if !expectedPaths[path] {
			t.Errorf("unexpected path: %s", path)
		}
	}
}

func TestHasPath(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	tests := []struct {
		path     string
		expected bool
	}{
		{"/v1/blocks", true},
		{"/v1/blocks/{id}", true},
		{"/v1/actions", true},
		{"/v1/health", true},
		{"/v1/admin", true},
		{"/v1/nonexistent", false},
		{"/v2/blocks", false},
		{"", false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			if got := spec.HasPath(tt.path); got != tt.expected {
				t.Errorf("HasPath(%q) = %v, want %v", tt.path, got, tt.expected)
			}
		})
	}
}

func TestPathMethods(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	tests := []struct {
		path     string
		expected []string
	}{
		{"/v1/blocks", []string{"GET"}},
		{"/v1/blocks/{id}", []string{"GET"}},
		{"/v1/actions", []string{"GET", "POST"}},
		{"/v1/health", []string{"GET", "HEAD"}},
		{"/v1/admin", []string{"PUT", "DELETE", "PATCH", "OPTIONS"}},
		{"/v1/nonexistent", nil},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			methods := spec.PathMethods(tt.path)

			if tt.expected == nil {
				if methods != nil {
					t.Errorf("PathMethods(%q) = %v, want nil", tt.path, methods)
				}
				return
			}

			if len(methods) != len(tt.expected) {
				t.Errorf("PathMethods(%q) returned %d methods, want %d: got %v", tt.path, len(methods), len(tt.expected), methods)
				return
			}

			expectedSet := make(map[string]bool)
			for _, m := range tt.expected {
				expectedSet[m] = true
			}

			for _, m := range methods {
				if !expectedSet[m] {
					t.Errorf("PathMethods(%q) returned unexpected method %s", tt.path, m)
				}
			}
		})
	}
}

func TestValidateRoutes_AllPresent(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	checker := func(path, method string) bool {
		return true
	}

	result := spec.ValidateRoutes(checker)
	if !result.Valid {
		t.Error("expected Valid=true when all routes present")
	}
	if len(result.MissingHandlers) > 0 {
		t.Errorf("expected no missing handlers, got %v", result.MissingHandlers)
	}
}

func TestValidateRoutes_SomeMissing(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	checker := func(path, method string) bool {
		return path != "/v1/admin"
	}

	result := spec.ValidateRoutes(checker)
	if result.Valid {
		t.Error("expected Valid=false when routes missing")
	}
	if len(result.MissingHandlers) == 0 {
		t.Error("expected missing handlers list")
	}

	foundAdminMissing := false
	for _, h := range result.MissingHandlers {
		if strings.Contains(h, "/v1/admin") {
			foundAdminMissing = true
			break
		}
	}
	if !foundAdminMissing {
		t.Error("expected /v1/admin routes to be in missing handlers")
	}
}

func TestValidateRoutes_AllMissing(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	checker := func(path, method string) bool {
		return false
	}

	result := spec.ValidateRoutes(checker)
	if result.Valid {
		t.Error("expected Valid=false when all routes missing")
	}
	if len(result.MissingHandlers) == 0 {
		t.Error("expected missing handlers")
	}
}

func TestHandler_JSON(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	handler := spec.Handler()

	req := httptest.NewRequest("GET", "/openapi.json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", contentType)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), `"openapi"`) {
		t.Error("response should contain JSON openapi key")
	}
}

func TestHandler_YAML_AcceptHeader(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	handler := spec.Handler()

	req := httptest.NewRequest("GET", "/openapi.yaml", nil)
	req.Header.Set("Accept", "application/x-yaml")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/x-yaml" {
		t.Errorf("expected Content-Type application/x-yaml, got %s", contentType)
	}

	body, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(body), "openapi:") {
		t.Error("response should contain YAML openapi key")
	}
}

func TestHandler_YAML_QueryParam(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	handler := spec.Handler()

	req := httptest.NewRequest("GET", "/openapi?format=yaml", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/x-yaml" {
		t.Errorf("expected Content-Type application/x-yaml, got %s", contentType)
	}
}

func TestFilteredHandler_FilterPaths(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return strings.HasPrefix(path, "/v1/blocks")
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi.json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "/v1/blocks") {
		t.Error("filtered response should contain /v1/blocks")
	}

	if strings.Contains(bodyStr, `"/v1/actions"`) {
		t.Error("filtered response should NOT contain /v1/actions")
	}

	if strings.Contains(bodyStr, `"/v1/health"`) {
		t.Error("filtered response should NOT contain /v1/health")
	}
}

func TestFilteredHandler_YAML(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return path == "/v1/health"
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi?format=yaml", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/x-yaml" {
		t.Errorf("expected Content-Type application/x-yaml, got %s", contentType)
	}

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "/v1/health") {
		t.Error("filtered response should contain /v1/health")
	}
}

func TestFilteredHandler_RemovesUnusedTags(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return path == "/v1/health"
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi.json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if strings.Contains(bodyStr, `"blocks"`) && strings.Contains(bodyStr, `"tags"`) {
		lines := strings.Split(bodyStr, "\n")
		for _, line := range lines {
			if strings.Contains(line, `"name"`) && strings.Contains(line, `"blocks"`) {
				t.Error("filtered response should NOT contain 'blocks' tag (unused after filtering)")
				break
			}
		}
	}
}

func TestFilteredHandler_RemovesUnusedSchemas(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return path == "/v1/health"
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi.json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if strings.Contains(bodyStr, "Block") {
		t.Error("filtered response should NOT contain 'Block' schema (unused after filtering)")
	}

	if strings.Contains(bodyStr, "ActionInput") {
		t.Error("filtered response should NOT contain 'ActionInput' schema (unused after filtering)")
	}

	if strings.Contains(bodyStr, "UnusedSchema") {
		t.Error("filtered response should NOT contain 'UnusedSchema'")
	}
}

func TestFilteredHandler_KeepsReferencedSchemas(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return path == "/v1/blocks"
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi.json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "BlockList") {
		t.Error("filtered response should contain 'BlockList' schema (referenced by /v1/blocks)")
	}
}

func TestFilteredHandler_AllFiltered(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return false
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi.json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200 even with all paths filtered, got %d", resp.StatusCode)
	}
}

func TestFilteredHandler_NoneFiltered(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return true
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi.json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "/v1/blocks") {
		t.Error("response should contain all paths when none filtered")
	}
	if !strings.Contains(bodyStr, "/v1/actions") {
		t.Error("response should contain all paths when none filtered")
	}
}

func TestModel(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	model := spec.Model()
	if model == nil {
		t.Fatal("Model() returned nil")
	}

	if model.Info == nil {
		t.Fatal("Model.Info is nil")
	}

	if model.Paths == nil {
		t.Fatal("Model.Paths is nil")
	}

	if model.Components == nil {
		t.Fatal("Model.Components is nil")
	}
}

const specWithNestedSchemas = `openapi: 3.0.3
info:
  title: Nested Schema Test
  version: 1.0.0
paths:
  /v1/nested:
    get:
      responses:
        '200':
          description: Success
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Parent'
components:
  schemas:
    Parent:
      type: object
      properties:
        child:
          $ref: '#/components/schemas/Child'
        children:
          type: array
          items:
            $ref: '#/components/schemas/Child'
    Child:
      type: object
      properties:
        name:
          type: string
        grandchild:
          $ref: '#/components/schemas/Grandchild'
    Grandchild:
      type: object
      properties:
        value:
          type: integer
    Unrelated:
      type: object
      properties:
        unused:
          type: boolean
`

func TestFilteredHandler_NestedSchemaRefs(t *testing.T) {
	spec, err := Load([]byte(specWithNestedSchemas))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return true
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi.json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "Parent") {
		t.Error("response should contain Parent schema")
	}

	if !strings.Contains(bodyStr, "Child") {
		t.Error("response should contain Child schema (nested ref)")
	}
}

const specWithAllOf = `openapi: 3.0.3
info:
  title: AllOf Test
  version: 1.0.0
paths:
  /v1/combined:
    get:
      responses:
        '200':
          description: Success
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/Combined'
components:
  schemas:
    Combined:
      allOf:
        - $ref: '#/components/schemas/Base'
        - type: object
          properties:
            extra:
              type: string
    Base:
      type: object
      properties:
        id:
          type: integer
`

func TestFilteredHandler_AllOfRefs(t *testing.T) {
	spec, err := Load([]byte(specWithAllOf))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return true
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi.json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "Combined") {
		t.Error("response should contain Combined schema")
	}

	if !strings.Contains(bodyStr, "Base") {
		t.Error("response should contain Base schema (allOf ref)")
	}
}

const specWithOneOfAnyOf = `openapi: 3.0.3
info:
  title: OneOf/AnyOf Test
  version: 1.0.0
paths:
  /v1/union:
    get:
      responses:
        '200':
          description: Success
          content:
            application/json:
              schema:
                oneOf:
                  - $ref: '#/components/schemas/TypeA'
                  - $ref: '#/components/schemas/TypeB'
    post:
      requestBody:
        content:
          application/json:
            schema:
              anyOf:
                - $ref: '#/components/schemas/TypeA'
                - $ref: '#/components/schemas/TypeC'
      responses:
        '201':
          description: Created
components:
  schemas:
    TypeA:
      type: object
      properties:
        a:
          type: string
    TypeB:
      type: object
      properties:
        b:
          type: string
    TypeC:
      type: object
      properties:
        c:
          type: string
`

func TestFilteredHandler_OneOfAnyOfRefs(t *testing.T) {
	spec, err := Load([]byte(specWithOneOfAnyOf))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return true
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi.json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "TypeA") {
		t.Error("response should contain TypeA schema")
	}

	if !strings.Contains(bodyStr, "TypeB") {
		t.Error("response should contain TypeB schema (oneOf ref)")
	}

	if !strings.Contains(bodyStr, "TypeC") {
		t.Error("response should contain TypeC schema (anyOf ref)")
	}
}

const specWithAdditionalProperties = `openapi: 3.0.3
info:
  title: AdditionalProperties Test
  version: 1.0.0
paths:
  /v1/map:
    get:
      responses:
        '200':
          description: Success
          content:
            application/json:
              schema:
                $ref: '#/components/schemas/MapType'
components:
  schemas:
    MapType:
      type: object
      additionalProperties:
        $ref: '#/components/schemas/ValueType'
    ValueType:
      type: object
      properties:
        value:
          type: string
`

func TestFilteredHandler_AdditionalPropertiesRefs(t *testing.T) {
	spec, err := Load([]byte(specWithAdditionalProperties))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return true
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi.json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "MapType") {
		t.Error("response should contain MapType schema")
	}

	if !strings.Contains(bodyStr, "ValueType") {
		t.Error("response should contain ValueType schema (additionalProperties ref)")
	}
}

func TestValidationResult_Fields(t *testing.T) {
	result := ValidationResult{
		Valid:            false,
		MissingHandlers:  []string{"GET /v1/test"},
		ExtraHandlers:    []string{"POST /v1/extra"},
		MethodMismatches: []string{"/v1/mismatch"},
	}

	if result.Valid {
		t.Error("expected Valid=false")
	}

	if len(result.MissingHandlers) != 1 {
		t.Errorf("expected 1 missing handler, got %d", len(result.MissingHandlers))
	}

	if len(result.ExtraHandlers) != 1 {
		t.Errorf("expected 1 extra handler, got %d", len(result.ExtraHandlers))
	}

	if len(result.MethodMismatches) != 1 {
		t.Errorf("expected 1 method mismatch, got %d", len(result.MethodMismatches))
	}
}

const specWithParameterSchema = `openapi: 3.0.3
info:
  title: Parameter Schema Test
  version: 1.0.0
paths:
  /v1/items/{id}:
    get:
      parameters:
        - name: id
          in: path
          required: true
          schema:
            $ref: '#/components/schemas/ItemID'
      responses:
        '200':
          description: Success
components:
  schemas:
    ItemID:
      type: string
      format: uuid
`

func TestFilteredHandler_ParameterSchemaRefs(t *testing.T) {
	spec, err := Load([]byte(specWithParameterSchema))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return true
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi.json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	bodyStr := string(body)

	if !strings.Contains(bodyStr, "ItemID") {
		t.Error("response should contain ItemID schema (parameter ref)")
	}
}

func TestHandler_AcceptHeaderJSON(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	handler := spec.Handler()

	req := httptest.NewRequest("GET", "/openapi", nil)
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/json" {
		t.Errorf("expected Content-Type application/json, got %s", contentType)
	}
}

func TestFilteredHandler_AcceptYAML(t *testing.T) {
	spec, err := Load([]byte(testSpecYAML))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return true
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi", nil)
	req.Header.Set("Accept", "text/yaml")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	contentType := resp.Header.Get("Content-Type")
	if contentType != "application/x-yaml" {
		t.Errorf("expected Content-Type application/x-yaml for Accept: text/yaml, got %s", contentType)
	}
}

const minimalSpec = `openapi: 3.0.3
info:
  title: Minimal
  version: 1.0.0
paths: {}
`

func TestLoad_MinimalSpec(t *testing.T) {
	spec, err := Load([]byte(minimalSpec))
	if err != nil {
		t.Fatalf("Load minimal spec failed: %v", err)
	}

	paths := spec.Paths()
	if len(paths) != 0 {
		t.Errorf("expected 0 paths, got %d", len(paths))
	}
}

func TestFilteredHandler_MinimalSpec(t *testing.T) {
	spec, err := Load([]byte(minimalSpec))
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}

	filter := func(path string) bool {
		return true
	}

	handler := spec.FilteredHandler(filter)

	req := httptest.NewRequest("GET", "/openapi.json", nil)
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	resp := w.Result()
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected status 200, got %d", resp.StatusCode)
	}
}
