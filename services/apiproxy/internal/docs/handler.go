package docs

import (
	"html/template"
	"net/http"
)

type Config struct {
	Title       string
	Description string
	SpecURL     string
	Theme       string
	DarkMode    bool
}

func Handler(cfg Config) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		tmpl.Execute(w, cfg)
	})
}

var tmpl = template.Must(template.New("scalar").Parse(`<!doctype html>
<html>
<head>
  <title>{{.Title}}</title>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <meta name="description" content="{{.Description}}" />
</head>
<body>
  <div id="app"></div>
  <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference"></script>
  <script>
    Scalar.createApiReference('#app', {
      url: '{{.SpecURL}}',
      darkMode: {{.DarkMode}},
      theme: '{{.Theme}}',
      layout: 'modern',
      hideTestRequestButton: false,
      showSidebar: true,
    })
  </script>
</body>
</html>`))
