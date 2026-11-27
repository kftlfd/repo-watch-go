package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

func TestDocsHandlerServesOpenAPISpecWithETag(t *testing.T) {
	handler, err := newDocsHandler("docs.example.test", "/custom/openapi.json")
	require.NoError(t, err)

	router := testDocsRouter(handler)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/docs/openapi.json", nil))

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, openAPISpecContentType, w.Header().Get("Content-Type"))
	etag := w.Header().Get("ETag")
	require.NotEmpty(t, etag)

	var spec map[string]any
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &spec))
	require.Equal(t, "docs.example.test", spec["host"])

	req := httptest.NewRequest(http.MethodGet, "/docs/openapi.json", nil)
	req.Header.Set("If-None-Match", etag)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotModified, w.Code)
	require.Empty(t, w.Body.String())
	require.Equal(t, etag, w.Header().Get("ETag"))
}

func TestDocsHandlerServesScalarDocsWithSpecPath(t *testing.T) {
	handler, err := newDocsHandler("docs.example.test", "/custom/openapi.json")
	require.NoError(t, err)

	router := testDocsRouter(handler)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/docs", nil))

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, scalarDocsContentType, w.Header().Get("Content-Type"))
	require.Contains(t, w.Body.String(), "/custom/openapi.json")
	require.Contains(t, w.Body.String(), "cdn.jsdelivr.net/npm/@scalar/api-reference")
	etag := w.Header().Get("ETag")
	require.NotEmpty(t, etag)

	req := httptest.NewRequest(http.MethodGet, "/docs", nil)
	req.Header.Set("If-None-Match", etag)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	require.Equal(t, http.StatusNotModified, w.Code)
	require.Empty(t, w.Body.String())
}

func TestDocsHandlerConditionalRequests(t *testing.T) {
	handler, err := newDocsHandler("docs.example.test", "/custom/openapi.json")
	require.NoError(t, err)

	router := testDocsRouter(handler)

	w := httptest.NewRecorder()
	router.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/docs/openapi.json", nil))
	etag := w.Header().Get("ETag")
	require.NotEmpty(t, etag)

	tests := []struct {
		name       string
		value      string
		wantStatus int
	}{
		{name: "weak match", value: "W/" + etag, wantStatus: http.StatusNotModified},
		{name: "list match", value: `"other", ` + etag, wantStatus: http.StatusNotModified},
		{name: "wildcard", value: "*", wantStatus: http.StatusNotModified},
		{name: "different", value: `"other"`, wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/docs/openapi.json", nil)
			req.Header.Set("If-None-Match", tt.value)
			w := httptest.NewRecorder()
			router.ServeHTTP(w, req)

			require.Equal(t, tt.wantStatus, w.Code)
			require.Equal(t, etag, w.Header().Get("ETag"))
		})
	}
}

func TestDocsHandlerHead(t *testing.T) {
	handler, err := newDocsHandler("docs.example.test", "/custom/openapi.json")
	require.NoError(t, err)

	router := testDocsRouter(handler)

	for _, target := range []string{"/docs", "/docs/openapi.json"} {
		w := httptest.NewRecorder()
		router.ServeHTTP(w, httptest.NewRequest(http.MethodHead, target, nil))

		require.Equal(t, http.StatusOK, w.Code)
		require.Empty(t, w.Body.String())
		require.NotEmpty(t, w.Header().Get("ETag"))
		require.Equal(t, "no-cache", w.Header().Get("Cache-Control"))
	}
}

func testDocsRouter(handler *docsHandler) *gin.Engine {
	gin.SetMode(gin.TestMode)

	router := gin.New()
	router.GET("/docs", handler.serveScalarDocs)
	router.HEAD("/docs", handler.serveScalarDocs)
	router.GET("/docs/openapi.json", handler.serveOpenAPISpec)
	router.HEAD("/docs/openapi.json", handler.serveOpenAPISpec)
	return router
}
