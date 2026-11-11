package subs

import (
	"fmt"
	"net/mail"
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

type emailInfo struct {
	email string
}

// NormalizeEmail returns the canonical form used for storing and looking up
// subscriptions: the bare address, lowercased. Display-name forms are rejected.
func NormalizeEmail(emailStr string) (string, error) {
	emailStr = strings.TrimSpace(emailStr)
	if emailStr == "" {
		return "", fmt.Errorf("empty email")
	}

	addr, err := mail.ParseAddress(emailStr)
	if err != nil {
		return "", fmt.Errorf("invalid email: %w", err)
	}

	if addr.Address != emailStr {
		return "", fmt.Errorf("email must be a bare address")
	}

	email := strings.ToLower(addr.Address)
	if email == "" {
		return "", fmt.Errorf("empty email")
	}

	return email, nil
}

func verifyEmailInfo(emailStr string) (emailInfo, error) {
	email, err := NormalizeEmail(emailStr)
	if err != nil {
		return emailInfo{}, err
	}

	return emailInfo{email: email}, nil
}
