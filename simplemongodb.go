// Package simplemongodb provides a struct that wraps around a single DB with a single mongodb client.
package simplemongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// New is a constructor.
func New() SimpleMongoDB {
	return &simpleMongoDB{}
}

// SimpleMongoDB wraps around a single DB with a single mongodb client.
type SimpleMongoDB interface {
	Connect(ctx context.Context, uri, nameDB string, nameCollections ...string) error
	Disconnect(ctx context.Context) error
	IsConnected() bool
	Client() *mongo.Client
	Database() *mongo.Database
	Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection
}

// simpleMongoDB wraps around a single DB with a single mongodb client.
// Zero value of this is ready to use.
type simpleMongoDB struct {
	mu            sync.RWMutex
	client        *mongo.Client
	database      *mongo.Database
	collections   map[string]*mongo.Collection
	collectionsMu sync.RWMutex
}

func (smdb *simpleMongoDB) isConnected() bool {
	return smdb.client != nil
}

// Disconnect from client and delete its object.
// If client is nil then this procedure is noop.
func (smdb *simpleMongoDB) disconnect() {
	if smdb.isConnected() {
		for smdb.client.Disconnect(context.Background()) != nil {
		}
		// disposal
		smdb.client = nil
		smdb.database = nil
		smdb.collections = nil
	}
}

// Connect to a single DB with a single mongo client.
// Errors if it cannot reach any of desired database or collections.
// Call Disconnect method to close down connection.
func (smdb *simpleMongoDB) Connect(ctx context.Context, uri, nameDB string, nameCollections ...string) error {
	// write lock
	smdb.mu.Lock()
	defer smdb.mu.Unlock()
	// validate
	if smdb.isConnected() {
		return errors.New("already connected")
	}
	{ // construct a client
		// the return is assign to a local variable in case of error violating the global
		mdc, errClient := mongo.NewClient(options.Client().
			ApplyURI(uri).
			SetRetryWrites(false))
		if errClient != nil {
			return fmt.Errorf("creating client: %v", errClient)
		}
		smdb.client = mdc
	}
	// connect
	if errConnect := smdb.client.Connect(ctx); errConnect != nil {
		return fmt.Errorf("connecting client: %v", errConnect)
	}
	// ping
	if errPing := smdb.client.Ping(ctx, nil); errPing != nil {
		smdb.disconnect()
		return fmt.Errorf("sending ping: %v", errPing)
	}
	// get database
	smdb.database = smdb.client.Database(nameDB)
	if smdb.database == nil {
		smdb.disconnect()
		return fmt.Errorf("cannot get database: %v", nameDB)
	}
	// set collection
	smdb.collectionsMu.Lock()
	smdb.collections = map[string]*mongo.Collection{}
	smdb.collectionsMu.Unlock()
	for _, nameCollection := range nameCollections {
		collection := smdb.database.Collection(nameCollection)
		if collection == nil {
			smdb.disconnect()
			return fmt.Errorf("cannot get collection: %v", nameCollection)
		}
		smdb.collectionsMu.Lock()
		smdb.collections[nameCollection] = smdb.database.Collection(nameCollection)
		smdb.collectionsMu.Unlock()
	}
	// return
	return nil
}

// Disconnect the connection to DB.
func (smdb *simpleMongoDB) Disconnect(ctx context.Context) error {
	// write lock
	smdb.mu.Lock()
	defer smdb.mu.Unlock()
	if !smdb.isConnected() {
		return errors.New("not connected")
	}
	return smdb.client.Disconnect(ctx)
}

// IsConnected tells if this is connected.
func (smdb *simpleMongoDB) IsConnected() bool {
	// read lock
	smdb.mu.RLock()
	defer smdb.mu.RUnlock()
	return smdb.isConnected()
}

// Client is a getter returning a mongo client.
// This returns nil if not connected.
func (smdb *simpleMongoDB) Client() *mongo.Client {
	// read lock
	smdb.mu.RLock()
	defer smdb.mu.RUnlock()
	if !smdb.isConnected() {
		return nil
	}
	return smdb.client
}

// Database is a getter returning an object representation of a DB.
// This returns nil if not connected.
func (smdb *simpleMongoDB) Database() *mongo.Database {
	// read lock
	smdb.mu.RLock()
	defer smdb.mu.RUnlock()
	if !smdb.isConnected() {
		return nil
	}
	return smdb.database
}

// Collection gets a collection from DB efficiently as it uses a map to cache collection objects.
// This returns nil if not connected or collection is not present in the database.
func (smdb *simpleMongoDB) Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection {
	// read lock
	smdb.mu.RLock()
	defer smdb.mu.RUnlock()
	if !smdb.isConnected() {
		return nil
	}
	// read lock
	smdb.collectionsMu.RLock()
	ret, ok := smdb.collections[name]
	smdb.collectionsMu.RUnlock()
	if !ok {
		ret = smdb.database.Collection(name, opts...)
		// write lock
		smdb.collectionsMu.Lock()
		smdb.collections[name] = ret
		smdb.collectionsMu.Unlock()
	}
	return ret
}
