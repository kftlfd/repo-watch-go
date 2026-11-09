package web

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"io"
	"io/fs"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
)

//go:embed static
var staticFS embed.FS

var staticFiles = newStaticCache(mustSubFS(staticFS, "static"))

type staticCache struct {
	fs    fs.FS
	mu    sync.RWMutex
	etags map[string]string
}

func newStaticCache(fsys fs.FS) *staticCache {
	return &staticCache{
		fs:    fsys,
		etags: make(map[string]string),
	}
}

func mustSubFS(fsys fs.FS, dir string) fs.FS {
	sub, err := fs.Sub(fsys, dir)
	if err != nil {
		panic(err)
	}

	return sub
}

func (c *staticCache) get(name string) (string, bool) {
	c.mu.RLock()
	etag, ok := c.etags[name]
	c.mu.RUnlock()
	if ok {
		return etag, true
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if etag, ok := c.etags[name]; ok {
		return etag, true
	}

	f, err := c.fs.Open(name)
	if err != nil {
		return "", false
	}
	defer f.Close()

	sum := sha256.New()
	if _, err := io.Copy(sum, f); err != nil {
		return "", false
	}

	etag = `"` + hex.EncodeToString(sum.Sum(nil)) + `"`
	c.etags[name] = etag

	return etag, true
}

func (h *webHandler) handleStatic(c *gin.Context) {
	name := strings.TrimPrefix(c.Param("filepath"), "/")
	if !fs.ValidPath(name) {
		staticNotFound(c)
		return
	}

	etag, ok := staticFiles.get(name)
	if !ok {
		staticNotFound(c)
		return
	}

	f, err := staticFiles.fs.Open(name)
	if err != nil {
		staticNotFound(c)
		return
	}
	defer f.Close()

	content, ok := f.(io.ReadSeeker)
	if !ok {
		c.Status(http.StatusInternalServerError)
		return
	}

	c.Header("Cache-Control", "no-cache")
	c.Header("ETag", etag)
	c.Header("X-Content-Type-Options", "nosniff")

	http.ServeContent(c.Writer, c.Request, name, time.Time{}, content)
}

func staticNotFound(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	c.Status(http.StatusNotFound)
}
