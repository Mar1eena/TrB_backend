package investgo

import (
	"crypto/x509"
	"embed"
	"fmt"
)

//go:embed certs/*.crt
var russianTrustedCAPEMs embed.FS

// appendRussianTrustedCAs добавляет сертификаты НУЦ Минцифры в pool.
// Нужны для TLS к T-Invest API в окружениях без этих CA (Docker scratch и т.п.).
func appendRussianTrustedCAs(pool *x509.CertPool) error {
	entries, err := russianTrustedCAPEMs.ReadDir("certs")
	if err != nil {
		return fmt.Errorf("чтение встроенных CA: %w", err)
	}
	added := 0
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		data, err := russianTrustedCAPEMs.ReadFile("certs/" + entry.Name())
		if err != nil {
			return fmt.Errorf("чтение %s: %w", entry.Name(), err)
		}
		if pool.AppendCertsFromPEM(data) {
			added++
		}
	}
	if added == 0 {
		return fmt.Errorf("не удалось добавить ни одного встроенного CA")
	}
	return nil
}
