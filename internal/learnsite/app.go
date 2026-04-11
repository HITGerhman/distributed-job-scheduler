package learnsite

import (
	"io/fs"
	"log"
	"net/http"
	"os"

	"djs/internal/config"
)

type App struct {
	Handler http.Handler
	service *Service
}

func NewApp(static fs.FS, configPath string, logger *log.Logger) (*App, error) {
	cfg, err := config.Load(configPath)
	if err != nil {
		return nil, err
	}
	rootDir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	service, err := NewService(cfg, rootDir, configPath)
	if err != nil {
		return nil, err
	}
	return &App{
		Handler: NewHandler(static, service, logger),
		service: service,
	}, nil
}

func (a *App) Close() error {
	if a == nil || a.service == nil {
		return nil
	}
	return a.service.Close()
}
