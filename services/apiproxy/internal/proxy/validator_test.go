package proxy

import (
	"net/url"
	"testing"
)

func TestValidateQuery(t *testing.T) {
	v := NewRouteValidator(&ValidatorConfig{
		AllowedQueryParams: []string{"limit", "cursor", "order"},
	})

	tests := []struct {
		name  string
		query string
		want  bool
	}{
		{"empty query", "", true},
		{"valid single", "limit=100", true},
		{"valid multiple", "limit=100&cursor=abc&order=desc", true},
		{"invalid param", "limit=100&invalid=foo", false},
		{"all invalid", "foo=bar&baz=qux", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q, _ := url.ParseQuery(tt.query)
			got := v.ValidateQuery(q)
			if got != tt.want {
				t.Errorf("ValidateQuery(%q) = %v, want %v", tt.query, got, tt.want)
			}
		})
	}
}

func TestValidateBody(t *testing.T) {
	v := NewRouteValidator(&ValidatorConfig{
		AllowedBodyKeys: []string{"account_name", "limit", "cursor"},
	})

	tests := []struct {
		name string
		body string
		want bool
	}{
		{"empty body", "", true},
		{"valid single key", `{"account_name":"test"}`, true},
		{"valid multiple keys", `{"account_name":"test","limit":100,"cursor":"abc"}`, true},
		{"invalid key", `{"account_name":"test","invalid":"foo"}`, false},
		{"nested object top-level only", `{"account_name":"test","data":{"nested":"value"}}`, false},
		{"whitespace", `{ "account_name" : "test" }`, true},
		{"escaped quote in value", `{"account_name":"test\"value"}`, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := v.ValidateBody([]byte(tt.body))
			if got != tt.want {
				t.Errorf("ValidateBody(%q) = %v, want %v", tt.body, got, tt.want)
			}
		})
	}
}

func TestExtractJSONKeys(t *testing.T) {
	tests := []struct {
		name string
		body string
		want []string
	}{
		{"empty", "", nil},
		{"single key", `{"foo":"bar"}`, []string{"foo"}},
		{"multiple keys", `{"foo":"bar","baz":123}`, []string{"foo", "baz"}},
		{"nested only extracts top level", `{"foo":{"nested":"value"}}`, []string{"foo"}},
		{"array", `{"items":[1,2,3]}`, []string{"items"}},
		{"escaped", `{"key":"val\"ue"}`, []string{"key"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractJSONKeys([]byte(tt.body))
			if len(got) != len(tt.want) {
				t.Errorf("extractJSONKeys(%q) = %v, want %v", tt.body, got, tt.want)
				return
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Errorf("extractJSONKeys(%q)[%d] = %q, want %q", tt.body, i, got[i], tt.want[i])
				}
			}
		})
	}
}

func BenchmarkValidateQuery(b *testing.B) {
	v := NewRouteValidator(&ValidatorConfig{
		AllowedQueryParams: []string{"limit", "cursor", "order", "contract", "action", "decode"},
	})
	q, _ := url.ParseQuery("limit=100&cursor=abc123&order=desc&contract=eosio.token")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.ValidateQuery(q)
	}
}

func BenchmarkValidateBody(b *testing.B) {
	v := NewRouteValidator(&ValidatorConfig{
		AllowedBodyKeys: []string{"account_name", "pos", "offset", "filter", "sort"},
	})
	body := []byte(`{"account_name":"eosio","pos":-1,"offset":-100}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.ValidateBody(body)
	}
}

func BenchmarkValidateBodyLarge(b *testing.B) {
	v := NewRouteValidator(&ValidatorConfig{
		AllowedBodyKeys: []string{"code", "scope", "table", "lower_bound", "upper_bound", "limit", "json"},
	})
	body := []byte(`{"code":"eosio.token","scope":"eosio","table":"accounts","lower_bound":"","upper_bound":"","limit":1000,"json":true}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v.ValidateBody(body)
	}
}

func BenchmarkExtractJSONKeys(b *testing.B) {
	body := []byte(`{"account_name":"eosio","pos":-1,"offset":-100,"filter":"","sort":"desc"}`)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		extractJSONKeys(body)
	}
}
