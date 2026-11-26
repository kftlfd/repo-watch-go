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
	// Optional machine-readable error code for authentication failures.
	Code string `json:"code,omitempty" example:"invalid_credentials"`
}

// signupRequest is the payload accepted by POST /api/auth/signup.
type signupRequest struct {
	// Email address for the new account.
	Email string `json:"email" form:"email" binding:"required,email" example:"alice@example.com"`
	// Password
	Password string `json:"password" form:"password" binding:"required" example:"correct horse battery staple"`
}

// resendVerificationRequest is the payload accepted by
// POST /api/auth/verify-resend.
type resendVerificationRequest struct {
	// Email address to resend verification to.
	Email string `json:"email" form:"email" binding:"required,email" example:"alice@example.com"`
}

// loginRequest is the payload accepted by POST /api/auth/login.
type loginRequest struct {
	// Account email address.
	Email string `json:"email" form:"email" binding:"required,email" example:"alice@example.com"`
	// Account password.
	Password string `json:"password" form:"password" binding:"required" example:"correct horse battery staple"`
}

// refreshRequest is the payload accepted by POST /api/auth/refresh.
type refreshRequest struct {
	// Refresh token returned by login or a previous refresh.
	RefreshToken string `json:"refresh_token" form:"refresh_token" binding:"required" example:"..."`
}

// logoutRequest is the optional payload accepted by POST /api/auth/logout
// when the access token has expired.
type logoutRequest struct {
	// Refresh token used to identify the session to revoke.
	RefreshToken string `json:"refresh_token" form:"refresh_token" example:"..."`
}

// userResponse describes an authenticated account.
type userResponse struct {
	// Normalized account email address.
	Email string `json:"email" example:"alice@example.com"`
}

// tokenResponse is returned by login and refresh.
type tokenResponse struct {
	// Authenticated user.
	User userResponse `json:"user"`
	// Short-lived JWT access token.
	AccessToken string `json:"access_token" example:"..."`
	// Token type; always Bearer.
	TokenType string `json:"token_type" example:"Bearer"`
	// Access-token lifetime in seconds.
	ExpiresIn int64 `json:"expires_in" example:"600"`
	// Rotating refresh token.
	RefreshToken string `json:"refresh_token" example:"..."`
	// Refresh-token lifetime in seconds.
	RefreshExpiresIn int64 `json:"refresh_expires_in" example:"2592000"`
}

// createSubscriptionRequest is the payload accepted by POST /api/subscribe.
// It can be sent as JSON or as an application/x-www-form-urlencoded form.
// Email is required for anonymous requests and must match the session user when
// supplied to an authenticated request.
type createSubscriptionRequest struct {
	// Email address to subscribe. Required for anonymous requests; when authenticated it must match the session email.
	Email string `json:"email" form:"email" example:"alice@example.com"`
	// GitHub repository in "owner/name" format.
	Repo string `json:"repo" form:"repo" binding:"required" example:"golang/go"`
}

// unsubscribeRequest is the payload accepted by
// POST /api/subscriptions/unsubscribe.
type unsubscribeRequest struct {
	// GitHub repository in "owner/name" format.
	Repo string `json:"repo" form:"repo" binding:"required" example:"golang/go"`
}

type subscriptionResponse struct {
	// GitHub repository in "owner/name" format.
	Repo string `json:"repo" example:"golang/go"`
	// Whether the subscription is active. The list endpoint only returns active subscriptions, so this is always true.
	Confirmed bool `json:"confirmed" example:"true"`
}
