package web

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

type countingFS struct {
	fs.FS
	opens int
}

func (f *countingFS) Open(name string) (fs.File, error) {
	f.opens++
	return f.FS.Open(name)
}

func TestStaticCacheComputesHashOnce(t *testing.T) {
	content := []byte("body { color: red; }")
	fsys := &countingFS{FS: fstest.MapFS{
		"app.css": &fstest.MapFile{Data: content},
	}}
	cache := newStaticCache(fsys)

	first, ok := cache.get("app.css")
	require.True(t, ok)
	second, ok := cache.get("app.css")
	require.True(t, ok)

	require.Equal(t, first, second)
	require.Equal(t, 1, fsys.opens)

	sum := sha256.Sum256(content)
	require.Equal(t, `"`+hex.EncodeToString(sum[:])+`"`, first)
}

func TestStaticFileServesWithETag(t *testing.T) {
	h, _ := newTestRouter(t, &fakeSubsService{})

	w := doRequest(t, h, http.MethodGet, "/static/app.css", "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "no-cache", w.Header().Get("Cache-Control"))
	require.Equal(t, "nosniff", w.Header().Get("X-Content-Type-Options"))
	require.Contains(t, w.Header().Get("Content-Type"), "text/css")
	require.Contains(t, w.Body.String(), "font-family")

	etag := w.Header().Get("ETag")
	require.NotEmpty(t, etag)

	req := httptest.NewRequest(http.MethodGet, "/static/app.css", nil)
	req.Header.Set("If-None-Match", etag)
	notModified := httptest.NewRecorder()
	h.ServeHTTP(notModified, req)

	require.Equal(t, http.StatusNotModified, notModified.Code)
	require.Empty(t, notModified.Body.String())
	require.Equal(t, etag, notModified.Header().Get("ETag"))
	require.Equal(t, "no-cache", notModified.Header().Get("Cache-Control"))
}

func TestMissingStaticFileIsNotStored(t *testing.T) {
	h, _ := newTestRouter(t, &fakeSubsService{})

	w := doRequest(t, h, http.MethodGet, "/static/missing.css", "", "")

	require.Equal(t, http.StatusNotFound, w.Code)
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
}

func TestHTMLRoutesAreNotStored(t *testing.T) {
	h, _ := newTestRouter(t, &fakeSubsService{})

	w := doRequest(t, h, http.MethodGet, "/", "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
}

func TestStaticFileConditionalRequests(t *testing.T) {
	h, _ := newTestRouter(t, &fakeSubsService{})

	w := doRequest(t, h, http.MethodGet, "/static/app.css", "", "")
	etag := w.Header().Get("ETag")
	require.NotEmpty(t, etag)

	tests := []struct {
		name       string
		header     string
		value      string
		wantStatus int
	}{
		{name: "weak match", header: "If-None-Match", value: "W/" + etag, wantStatus: http.StatusNotModified},
		{name: "list match", header: "If-None-Match", value: `"other", ` + etag, wantStatus: http.StatusNotModified},
		{name: "wildcard", header: "If-None-Match", value: "*", wantStatus: http.StatusNotModified},
		{name: "no match", header: "If-None-Match", value: `"other"`, wantStatus: http.StatusOK},
		{name: "failed if-match", header: "If-Match", value: `"other"`, wantStatus: http.StatusPreconditionFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/static/app.css", nil)
			req.Header.Set(tt.header, tt.value)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, req)

			require.Equal(t, tt.wantStatus, w.Code)
			require.Equal(t, etag, w.Header().Get("ETag"))
			if tt.wantStatus == http.StatusNotModified {
				require.Empty(t, w.Body.String())
			}
		})
	}
}

func TestStaticFileHead(t *testing.T) {
	h, _ := newTestRouter(t, &fakeSubsService{})

	w := doRequest(t, h, http.MethodHead, "/static/app.css", "", "")

	require.Equal(t, http.StatusOK, w.Code)
	require.Empty(t, w.Body.String())
	require.Contains(t, w.Header().Get("Content-Type"), "text/css")
	require.NotEmpty(t, w.Header().Get("ETag"))
	require.Equal(t, "no-cache", w.Header().Get("Cache-Control"))
}
