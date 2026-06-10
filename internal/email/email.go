package email

import (
	"fmt"
	"net/smtp"
	"os"
)

var (
	smtpHost = getenv("SMTP_HOST", "localhost")
	smtpPort = getenv("SMTP_PORT", "1025")
	from     = getenv("SMTP_FROM", "noreply@promocoes.local")
)

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func Send(to, subject, body string) error {
	addr := smtpHost + ":" + smtpPort
	var msg []byte
	msg = fmt.Appendf(msg,
		"From: %s\r\nTo: %s\r\nSubject: %s\r\nContent-Type: text/plain; charset=UTF-8\r\n\r\n%s",
		from, to, subject, body,
	)
	return smtp.SendMail(addr, nil, from, []string{to}, msg)
}
