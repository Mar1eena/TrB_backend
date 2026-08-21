package investgo

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"

	yaml "gopkg.in/yaml.v3"

	"github.com/Mar1eena/TrB_V3/internal/pkg/env"
)

// Config - структура для кофигурации SDK
type Config struct {
	// EndPoint - Для работы с реальным контуром и контуром песочницы нужны разные эндпоинты.
	// По умолчанию = sandbox-invest-public-api.tbank.ru:443
	EndPoint string `yaml:"EndPoint"`
	// Token - Ваш токен для T-Bank InvestAPI
	Token string `yaml:"APIToken"`
	// AppName - Название вашего приложения, по умолчанию = tinkoff-api-go-sdk
	AppName string `yaml:"AppName"`
	// AccountId - Если уже есть аккаунт для апи можно указать напрямую,
	// по умолчанию откроется новый счет в песочнице
	AccountId string `yaml:"AccountId"`
	// DisableResourceExhaustedRetry - Если true, то сдк не пытается ретраить, после получения ошибки об исчерпывании
	// лимита запросов, если false, то сдк ждет нужное время и пытается выполнить запрос снова. По умолчанию = false
	DisableResourceExhaustedRetry bool `yaml:"DisableResourceExhaustedRetry"`
	// DisableAllRetry - Отключение всех ретраев
	DisableAllRetry bool `yaml:"DisableAllRetry"`
	// MaxRetries - Максимальное количество попыток переподключения, по умолчанию = 3
	// (если указать значение 0 это не отключит ретраи, для отключения нужно прописать DisableAllRetry = true)
	MaxRetries uint `yaml:"MaxRetries"`
	// TLSCertFile - Путь к файлу сертификата для TLS соединения (опционально)
	TLSCertFile string `yaml:"TLSCertFile"`
	// TLSKeyFile - Путь к файлу приватного ключа для TLS соединения (опционально)
	TLSKeyFile string `yaml:"TLSKeyFile"`
	// TLSCACertFile - Путь к файлу корневого сертификата CA для TLS соединения (опционально)
	TLSCACertFile string `yaml:"TLSCACertFile"`
	// InsecureSkipVerify - Пропустить проверку сертификата сервера (не рекомендуется для продакшена)
	InsecureSkipVerify bool `yaml:"InsecureSkipVerify"`
}

// LoadConfig - загрузка конфигурации для сдк из .yaml файла
func LoadConfig(filename string) (Config, error) {
	var c Config
	input, err := os.ReadFile(filename)
	if err != nil {
		return Config{}, err
	}
	err = yaml.Unmarshal(input, &c)
	if err != nil {
		log.Println(err)
	}
	return c, nil
}

// LoadEnvConfig reads invest settings from the process environment.
// Call env.Load() once at service startup before using this.
func LoadEnvConfig() Config {
	return Config{
		AccountId:                     env.Get("INVEST_ACCOUNT_ID"),
		Token:                         env.Get("INVEST_TOKEN"),
		EndPoint:                      env.Get("INVEST_ENDPOINT"),
		AppName:                       env.Get("INVEST_APP_NAME"),
		DisableResourceExhaustedRetry: false,
		DisableAllRetry:               false,
		MaxRetries:                    3,
	}
}

// BuildTLSConfig - создание TLS конфигурации на основе настроек.
// Всегда включает системные CA и встроенные сертификаты НУЦ Минцифры
// (нужны для T-Invest в Docker/scratch, где их нет в системном store).
func (c *Config) BuildTLSConfig() (*tls.Config, error) {
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if err := appendRussianTrustedCAs(pool); err != nil {
		return nil, err
	}

	tlsConfig := &tls.Config{
		RootCAs:            pool,
		InsecureSkipVerify: c.InsecureSkipVerify,
	}

	// Загрузка клиентского сертификата и ключа
	if c.TLSCertFile != "" && c.TLSKeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.TLSCertFile, c.TLSKeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// Дополнительные CA из файла (опционально)
	if c.TLSCACertFile != "" {
		caCert, err := os.ReadFile(c.TLSCACertFile)
		if err != nil {
			return nil, err
		}
		if !pool.AppendCertsFromPEM(caCert) {
			return nil, fmt.Errorf("не удалось добавить CA из %s", c.TLSCACertFile)
		}
	}

	return tlsConfig, nil
}
