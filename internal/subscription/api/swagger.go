// Package api provides the HTTP API for repository release subscriptions.
//
// The API creates pending subscriptions, queues confirmation emails, confirms
// or deactivates subscriptions through single-use tokens, and lists active
// subscriptions for an email address. None of the API endpoints require
// authentication.
//
// @title           Subscription API
// @version         1.0.0
// @description     API for subscribing email addresses to GitHub repository releases.
// @BasePath        /api
// @schemes         http https
// @tag.name        Subscriptions
// @tag.description Create, confirm, unsubscribe from, and list repository release subscriptions.
package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/swaggo/swag"
)

const (
	openAPISpecPath = "/api/docs/openapi.json"
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
        url: '` + openAPISpecPath + `',
        // The reference is served by the API itself, so target the same
        // origin instead of the host baked into the generated specification.
        servers: [{ url: '/api' }],
      })
    </script>
  </body>
</html>
`

func serveScalarDocs(c *gin.Context) {
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(scalarDocsHTML))
}

func serveOpenAPISpec(c *gin.Context) {
	doc, err := swag.ReadDoc()
	if err != nil {
		c.JSON(http.StatusInternalServerError, errorResponse{Error: errInternal})
		return
	}

	c.Data(http.StatusOK, "application/json; charset=utf-8", []byte(doc))
}
