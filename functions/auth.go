package functions

type AuthorizationLevel string

const (
	Function  AuthorizationLevel = "FUNCTION"
	Anonymous AuthorizationLevel = "ANONYOMOUS"
	Admin     AuthorizationLevel = "ADMIN"
)
