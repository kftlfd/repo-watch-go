package subs

import (
	"fmt"
	"strings"
	"unicode"
)

// maxRepositoryLen matches GitHub's owner (39) + "/" + name (100) limit.
const maxRepositoryLen = 140

type repoInfo struct {
	repoId, owner, name string
}

func verifyRepoInfo(inpStr string) (repoInfo, error) {
	rid := repoInfo{}

	inpStr = strings.TrimSpace(inpStr)
	owner, repo, ok := strings.Cut(inpStr, "/")
	if !ok {
		return rid, fmt.Errorf("invalid format")
	}

	owner = strings.ToLower(strings.TrimSpace(owner))
	repo = strings.ToLower(strings.TrimSpace(repo))

	if owner == "" || repo == "" {
		return rid, fmt.Errorf("empty owner or repository")
	}

	if strings.Contains(repo, "/") {
		return rid, fmt.Errorf("invalid format")
	}

	if strings.IndexFunc(owner, unicode.IsSpace) >= 0 || strings.IndexFunc(repo, unicode.IsSpace) >= 0 {
		return rid, fmt.Errorf("invalid whitespace")
	}

	if len(owner)+len(repo)+1 > maxRepositoryLen {
		return rid, fmt.Errorf("repository is too long")
	}

	rid.repoId = owner + "/" + repo
	rid.owner = owner
	rid.name = repo
	return rid, nil
}
