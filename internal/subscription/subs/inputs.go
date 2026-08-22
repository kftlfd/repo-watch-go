package subs

import (
	"fmt"
	"net/mail"
	"strings"
)

type repoInfo struct {
	repoId, owner, name string
}

func verifyRepoInfo(inpStr string) (repoInfo, error) {
	rid := repoInfo{}

	owner, repo, ok := strings.Cut(inpStr, "/")
	if !ok {
		return rid, fmt.Errorf("invalid format")
	}

	owner = strings.ToLower(strings.TrimSpace(owner))
	repo = strings.ToLower(strings.TrimSpace(repo))

	if strings.Contains(repo, "/") {
		return rid, fmt.Errorf("invalid format")
	}

	rid.repoId = owner + "/" + repo
	rid.owner = owner
	rid.name = repo
	return rid, nil
}

type emailInfo struct {
	email string
}

func verifyEmailInfo(emailStr string) (emailInfo, error) {
	em := emailInfo{}

	emailStr = strings.TrimSpace(emailStr)
	if emailStr == "" {
		return em, fmt.Errorf("empty email")
	}
	if _, err := mail.ParseAddress(emailStr); err != nil {
		return em, fmt.Errorf("invalid email: %w", err)
	}

	em.email = emailStr
	return em, nil
}
