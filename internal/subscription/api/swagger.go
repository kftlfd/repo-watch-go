// Package api provides the HTTP API for repository release subscriptions.
//
// The API manages accounts with email verification and Bearer sessions. It
// creates pending subscriptions for anonymous callers and activates
// subscriptions immediately for authenticated callers, confirms or deactivates
// subscriptions through single-use tokens, and lists or deactivates the
// authenticated user's subscriptions. Protected routes require a Bearer access
// token.
//
// @title           Subscription API
// @version         1.0.0
// @description     API for managing accounts and subscribing to GitHub repository releases.
// @BasePath        /api
// @schemes         http https
// @tag.name        Auth
// @tag.description Create accounts, verify email addresses, and manage Bearer sessions.
// @tag.name        Subscriptions
// @tag.description Create, confirm, unsubscribe from, and list repository release subscriptions.
// @securityDefinitions.apikey BearerAuth
// @in              header
// @name            Authorization
// @description     JWT access token. Use the value returned by login or refresh with the "Bearer " prefix.
package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/swaggo/swag"

	"repo-watch/internal/subscription/api/docs"
)

const (
	scalarDocsContentType  = "text/html; charset=utf-8"
	openAPISpecContentType = "application/json; charset=utf-8"
)

const scalarDocsHTML = `<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1" />
    <title>Subscription API Reference</title>
  </head>
  <body>
    <div id="app"></div>
    <script src="https://cdn.jsdelivr.net/npm/@scalar/api-reference@1.77.0"></script>
    <script>
      Scalar.createApiReference('#app', {
        url: '%s',
        // The reference is served by the API itself, so target the same
        // origin instead of the host baked into the generated specification.
        servers: [{ url: '/api' }],
      })
    </script>
  </body>
</html>
`

// docsHandler serves the API reference and its OpenAPI specification. The
// specification is rendered once so the host override and content hash stay
// stable for the lifetime of the handler.
type docsHandler struct {
	scalarDocs      []byte
	scalarDocsETag  string
	openAPISpec     []byte
	openAPISpecETag string
}

// newDocsHandler builds a docs handler for docsHost. The generated Swagger
// document's host is overridden before the document is rendered and cached.
func newDocsHandler(docsHost, specPath string) (*docsHandler, error) {
	docs.SwaggerInfo.Host = docsHost

	doc, err := swag.ReadDoc()
	if err != nil {
		return nil, fmt.Errorf("read openapi document: %w", err)
	}

	scalarDocs := fmt.Appendf(nil, scalarDocsHTML, specPath)
	openAPISpec := []byte(doc)

	return &docsHandler{
		scalarDocs:      scalarDocs,
		scalarDocsETag:  contentETag(scalarDocs),
		openAPISpec:     openAPISpec,
		openAPISpecETag: contentETag(openAPISpec),
	}, nil
}

func (h *docsHandler) serveScalarDocs(c *gin.Context) {
	h.serve(c, "docs.html", scalarDocsContentType, h.scalarDocs, h.scalarDocsETag)
}

func (h *docsHandler) serveOpenAPISpec(c *gin.Context) {
	h.serve(c, "openapi.json", openAPISpecContentType, h.openAPISpec, h.openAPISpecETag)
}

func (h *docsHandler) serve(c *gin.Context, name, contentType string, body []byte, etag string) {
	c.Header("Cache-Control", "no-cache")
	c.Header("Content-Type", contentType)
	c.Header("ETag", etag)

	http.ServeContent(c.Writer, c.Request, name, time.Time{}, bytes.NewReader(body))
}

func contentETag(body []byte) string {
	sum := sha256.Sum256(body)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}
