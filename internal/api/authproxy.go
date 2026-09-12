package api

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
)

// Auth proxy: account creation and login are proxied through this server to
// Supabase's Admin API / password grant using the service key. GoTrue limits
// public /signup and /token calls per-IP; the service key path is not subject
// to those limits, so users can create accounts and sign in as often as they
// like. The service key never leaves the server.

type authCreds struct {
	Email    string `json:"email"`
	Password string `json:"password"`
}

func decodeAuthCreds(w http.ResponseWriter, r *http.Request) (*authCreds, bool) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, 64<<10))
	if err != nil {
		writeJSON(w, 400, E{"error": "invalid body"})
		return nil, false
	}
	var c authCreds
	if err := json.Unmarshal(raw, &c); err != nil {
		writeJSON(w, 400, E{"error": "invalid JSON body"})
		return nil, false
	}
	c.Email = strings.TrimSpace(strings.ToLower(c.Email))
	if c.Email == "" || !strings.Contains(c.Email, "@") || len(c.Email) > 254 {
		writeJSON(w, 400, E{"error": "a valid email is required"})
		return nil, false
	}
	if len(c.Password) < 6 {
		writeJSON(w, 400, E{"error": "password must be at least 6 characters"})
		return nil, false
	}
	if len(c.Password) > 128 {
		writeJSON(w, 400, E{"error": "password too long"})
		return nil, false
	}
	return &c, true
}

// handleAuthSignup creates an account and returns a fresh session so the user
// is signed in immediately.
func (s *Server) handleAuthSignup(w http.ResponseWriter, r *http.Request) {
	c, ok := decodeAuthCreds(w, r)
	if !ok {
		return
	}
	if err := s.auth.SupabaseSignup(r.Context(), c.Email, c.Password); err != nil {
		writeJSON(w, 409, E{"error": err.Error()})
		return
	}
	session, err := s.auth.SupabaseToken(r.Context(), c.Email, c.Password)
	if err != nil {
		writeJSON(w, 201, E{"ok": true, "email": c.Email, "message": "account created — sign in to continue"})
		return
	}
	writeJSON(w, 201, E{"ok": true, "email": c.Email, "session": session})
}

// handleAuthToken exchanges email/password for a session.
func (s *Server) handleAuthToken(w http.ResponseWriter, r *http.Request) {
	c, ok := decodeAuthCreds(w, r)
	if !ok {
		return
	}
	session, err := s.auth.SupabaseToken(r.Context(), c.Email, c.Password)
	if err != nil {
		writeJSON(w, 401, E{"error": err.Error()})
		return
	}
	writeJSON(w, 200, E{"session": session})
}