package openapi

import (
	"fmt"
	"net/http"
	"strings"
	"sync"

	"github.com/pb33f/libopenapi"
	"github.com/pb33f/libopenapi/datamodel/high/base"
	v3high "github.com/pb33f/libopenapi/datamodel/high/v3"
)

type Spec struct {
	doc      libopenapi.Document
	model    *v3high.Document
	yamlData []byte
	jsonData []byte

	mu sync.RWMutex
}

func Load(yamlData []byte) (*Spec, error) {
	return LoadWithVersion(yamlData, "")
}

func LoadWithVersion(yamlData []byte, version string) (*Spec, error) {
	doc, err := libopenapi.NewDocument(yamlData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse OpenAPI spec: %w", err)
	}

	model, err := doc.BuildV3Model()
	if err != nil {
		return nil, fmt.Errorf("failed to build OpenAPI model: %v", err)
	}

	if version != "" {
		model.Model.Info.Version = version
	}

	yamlData, err = model.Model.Render()
	if err != nil {
		return nil, fmt.Errorf("failed to render OpenAPI as YAML: %w", err)
	}

	jsonData, err := model.Model.RenderJSON("")
	if err != nil {
		return nil, fmt.Errorf("failed to render OpenAPI as JSON: %w", err)
	}

	return &Spec{
		doc:      doc,
		model:    &model.Model,
		yamlData: yamlData,
		jsonData: jsonData,
	}, nil
}

func (s *Spec) Model() *v3high.Document {
	return s.model
}

func (s *Spec) YAML() []byte {
	return s.yamlData
}

func (s *Spec) JSON() []byte {
	return s.jsonData
}

func (s *Spec) Paths() []string {
	var paths []string
	for path := range s.model.Paths.PathItems.FromOldest() {
		paths = append(paths, path)
	}
	return paths
}

func (s *Spec) HasPath(path string) bool {
	return s.model.Paths.PathItems.GetOrZero(path) != nil
}

func (s *Spec) PathMethods(path string) []string {
	item := s.model.Paths.PathItems.GetOrZero(path)
	if item == nil {
		return nil
	}

	var methods []string
	if item.Get != nil {
		methods = append(methods, "GET")
	}
	if item.Post != nil {
		methods = append(methods, "POST")
	}
	if item.Put != nil {
		methods = append(methods, "PUT")
	}
	if item.Delete != nil {
		methods = append(methods, "DELETE")
	}
	if item.Patch != nil {
		methods = append(methods, "PATCH")
	}
	if item.Head != nil {
		methods = append(methods, "HEAD")
	}
	if item.Options != nil {
		methods = append(methods, "OPTIONS")
	}
	return methods
}

type RouteChecker func(path string, method string) bool

type ValidationResult struct {
	Valid            bool
	MissingHandlers  []string
	ExtraHandlers    []string
	MethodMismatches []string
}

func (s *Spec) ValidateRoutes(checker RouteChecker) ValidationResult {
	result := ValidationResult{Valid: true}

	for path := range s.model.Paths.PathItems.FromOldest() {
		methods := s.PathMethods(path)

		for _, method := range methods {
			if !checker(path, method) {
				result.MissingHandlers = append(result.MissingHandlers,
					fmt.Sprintf("%s %s", method, path))
				result.Valid = false
			}
		}
	}

	return result
}

func (s *Spec) Handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		accept := r.Header.Get("Accept")
		format := r.URL.Query().Get("format")

		useYAML := format == "yaml" ||
			(format == "" && strings.Contains(accept, "yaml"))

		if useYAML {
			w.Header().Set("Content-Type", "application/x-yaml")
			w.Write(s.yamlData)
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Write(s.jsonData)
		}
	})
}

type PathFilter func(path string) bool

func (s *Spec) FilteredHandler(filter PathFilter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		defer s.mu.RUnlock()

		filteredYAML, filteredJSON, err := s.filterPaths(filter)
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to filter spec: %v", err), http.StatusInternalServerError)
			return
		}

		accept := r.Header.Get("Accept")
		format := r.URL.Query().Get("format")

		useYAML := format == "yaml" ||
			(format == "" && strings.Contains(accept, "yaml"))

		if useYAML {
			w.Header().Set("Content-Type", "application/x-yaml")
			w.Write(filteredYAML)
		} else {
			w.Header().Set("Content-Type", "application/json")
			w.Write(filteredJSON)
		}
	})
}

func (s *Spec) filterPaths(filter PathFilter) (yamlData, jsonData []byte, err error) {
	doc, err := libopenapi.NewDocument(s.yamlData)
	if err != nil {
		return nil, nil, err
	}

	model, err := doc.BuildV3Model()
	if err != nil {
		return nil, nil, err
	}

	pathsToRemove := []string{}
	for path := range model.Model.Paths.PathItems.FromOldest() {
		if !filter(path) {
			pathsToRemove = append(pathsToRemove, path)
		}
	}

	for _, path := range pathsToRemove {
		model.Model.Paths.PathItems.Delete(path)
	}

	tagsToKeep := make(map[string]bool)
	referencedSchemas := make(map[string]bool)

	for _, item := range model.Model.Paths.PathItems.FromOldest() {
		for _, op := range []*v3high.Operation{item.Get, item.Post, item.Put, item.Delete, item.Patch, item.Head, item.Options} {
			if op != nil {
				for _, tag := range op.Tags {
					tagsToKeep[tag] = true
				}
				collectSchemaRefs(op, referencedSchemas)
			}
		}
	}

	if model.Model.Components != nil && model.Model.Components.Schemas != nil {
		schemasToRemove := []string{}
		for name := range model.Model.Components.Schemas.FromOldest() {
			if !referencedSchemas[name] {
				schemasToRemove = append(schemasToRemove, name)
			}
		}
		for _, name := range schemasToRemove {
			model.Model.Components.Schemas.Delete(name)
		}
	}

	if len(model.Model.Tags) > 0 {
		filteredTags := make([]*base.Tag, 0)
		for _, tag := range model.Model.Tags {
			if tagsToKeep[tag.Name] {
				filteredTags = append(filteredTags, tag)
			}
		}
		model.Model.Tags = filteredTags
	}

	yamlData, err = model.Model.Render()
	if err != nil {
		return nil, nil, err
	}

	jsonData, err = model.Model.RenderJSON("")
	if err != nil {
		return nil, nil, err
	}

	return yamlData, jsonData, nil
}

func collectSchemaRefs(op *v3high.Operation, refs map[string]bool) {
	if op == nil {
		return
	}

	if op.RequestBody != nil {
		for _, content := range op.RequestBody.Content.FromOldest() {
			if content.Schema != nil {
				collectRefsFromSchema(content.Schema, refs)
			}
		}
	}

	for _, resp := range op.Responses.Codes.FromOldest() {
		if resp.Content != nil {
			for _, content := range resp.Content.FromOldest() {
				if content.Schema != nil {
					collectRefsFromSchema(content.Schema, refs)
				}
			}
		}
	}

	for _, param := range op.Parameters {
		if param.Schema != nil {
			collectRefsFromSchema(param.Schema, refs)
		}
	}
}

func collectRefsFromSchema(schema *base.SchemaProxy, refs map[string]bool) {
	if schema == nil {
		return
	}

	if schema.IsReference() {
		ref := schema.GetReference()
		if strings.HasPrefix(ref, "#/components/schemas/") {
			name := strings.TrimPrefix(ref, "#/components/schemas/")
			refs[name] = true
		}
	}

	s := schema.Schema()
	if s == nil {
		return
	}

	for _, prop := range s.Properties.FromOldest() {
		collectRefsFromSchema(prop, refs)
	}

	if s.Items != nil && s.Items.A != nil {
		collectRefsFromSchema(s.Items.A, refs)
	}

	for _, allOf := range s.AllOf {
		collectRefsFromSchema(allOf, refs)
	}
	for _, oneOf := range s.OneOf {
		collectRefsFromSchema(oneOf, refs)
	}
	for _, anyOf := range s.AnyOf {
		collectRefsFromSchema(anyOf, refs)
	}

	if s.AdditionalProperties != nil && s.AdditionalProperties.A != nil {
		collectRefsFromSchema(s.AdditionalProperties.A, refs)
	}
}
