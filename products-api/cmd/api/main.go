package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"products-api/internal/application"
	appHTTP "products-api/internal/infrastructure/http"
	"products-api/internal/infrastructure/mongodb"
)

func main() {
	client, db := connectDB()

	// Nos aseguramos de desconectar la BD solo cuando finalice main()
	defer func() {
		ctxDisconnect, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := client.Disconnect(ctxDisconnect); err != nil {
			log.Printf("Error disconnecting from MongoDB: %v", err)
		} else {
			log.Println("MongoDB client disconnected successfully.")
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	repo := mongodb.NewProductRepository(db)
	if err := repo.EnsureIndexesAndSeed(ctx); err != nil {
		log.Printf("Warning during indexing/seeding: %v", err)
	}
	useCase := application.NewGetProductUseCase(repo)
	productHandler := appHTTP.NewProductHandler(useCase)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", appHTTP.HealthCheck)
	mux.HandleFunc("GET /products/{productId}", productHandler.GetProduct)

	server := &http.Server{
		Addr:         ":8082",
		Handler:      mux,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		IdleTimeout:  15 * time.Second,
	}

	shutdownChan := make(chan os.Signal, 1)
	signal.Notify(shutdownChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Printf("Products API running on port %s", server.Addr)
		if err := server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("HTTP server error: %v", err)
		}
	}()

	<-shutdownChan
	log.Println("Stopping Products API gracefully...")

	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Forced shutdown error: %v", err)
	}

	log.Println("Server stopped correctly.")
}

func connectDB() (*mongo.Client, *mongo.Database) {
	mongoURI := os.Getenv("MONGO_URI")
	if mongoURI == "" {
		mongoURI = "mongodb://product:secret@localhost:27017/?authSource=admin"
	}
	log.Println("Connecting to the DB.....")
	clientOptions := options.Client().ApplyURI(mongoURI)
	client, err := mongo.Connect(clientOptions)
	if err != nil {
		log.Fatalf("MongoDB connection error: %v", err)
	}

	pingCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(pingCtx, nil); err != nil {
		log.Fatalf("MongoDB ping failed: %v", err)
	}

	log.Println("Connected to MongoDB successfully.")
	return client, client.Database("catalog_db")
}
