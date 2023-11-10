package coockie

import (
	"net/http"
	"time"
)

func GetValue(request *http.Request, name string) (string, error) {
	if cookie, err := request.Cookie(name); err != nil {
		return "", err
	} else {
		return cookie.Value, nil
	}
}

func Remove(responseWriter http.ResponseWriter, cookieName string) {
	// Delete cookies (set as expired).
	expire := time.Now().Add(-7 * 24 * time.Hour)
	cookie := http.Cookie{
		Name:    cookieName,
		Expires: expire,
		MaxAge:  -1,
	}
	http.SetCookie(responseWriter, &cookie)
}
