package emails

const EmailConfirmSubscription = "confirm-sub"

type EmailConfirmSubscriptionData struct {
	RepoID       string `json:"repo_id"`
	ConfirmToken string `json:"confirm_token"`
}

const EmailSubscribed = "subscribed"

type EmailSubscribedData struct {
	RepoID string `json:"repo_id"`
}

const EmailUnsubscribed = "unsubscribed"

type EmailUnsubscribedData struct {
	RepoID string `json:"repo_id"`
}

const EmailNewReleaseNotification = "new-release"

type EmailNewReleaseNotificationPayload struct {
	RepoID string `json:"repo_id"`
	Owner  string `json:"owner"`
	Name   string `json:"name"`
	Tag    string `json:"tag"`
}
