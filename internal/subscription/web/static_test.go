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
