package api

// messageResponse is a human-readable success response.
type messageResponse struct {
	// Human-readable result message.
	Message string `json:"message" example:"check email to confirm subscription"`
}

// errorResponse is returned for all API errors.
type errorResponse struct {
	// Human-readable error message.
	Error string `json:"error" example:"invalid email"`
}

// createSubscriptionRequest is the payload accepted by POST /api/subscribe.
// It can be sent as JSON or as an application/x-www-form-urlencoded form.
type createSubscriptionRequest struct {
	// Email address to subscribe.
	Email string `json:"email" form:"email" binding:"required,email" example:"alice@example.com"`
	// GitHub repository in "owner/name" format.
	Repo string `json:"repo" form:"repo" binding:"required" example:"golang/go"`
}

type subscriptionResponse struct {
	// Email address that owns the subscription.
	Email string `json:"email" example:"alice@example.com"`
	// GitHub repository in "owner/name" format.
	Repo string `json:"repo" example:"golang/go"`
	// Whether the subscription is active. The list endpoint only returns active subscriptions, so this is always true.
	Confirmed bool `json:"confirmed" example:"true"`
}
