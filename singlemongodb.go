// Package singlemongodb wraps around a single DB with a single mongodb client.
package singlemongodb

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// Singleton since we don't need multiple connections for a single app.
var defaultSingleMongoDB = &singleMongoDB{}

// Self returns this package as an object.
func Self() SingleMongoDB {
	return defaultSingleMongoDB
}

// New is a constructor you wouldn't want to use at all.
func New() SingleMongoDB {
	return &singleMongoDB{}
}

// Connect to a single DB with a single mongo client.
func Connect(ctx context.Context, uri, nameDB string, nameCollections ...string) error {
	return defaultSingleMongoDB.Connect(ctx, uri, nameDB, nameCollections...)
}

// Disconnect the connection to DB.
func Disconnect(ctx context.Context) error {
	return defaultSingleMongoDB.Disconnect(ctx)
}

// Client is a getter returning a mongo client.
// This returns nil if not connected.
func Client() *mongo.Client {
	return defaultSingleMongoDB.Client()
}

// Database is a getter returning an object representation of a DB.
// This returns nil if not connected.
func Database() *mongo.Database {
	return defaultSingleMongoDB.Database()
}

// Collection gets a collection from DB efficiently as it uses a map to cache collection objects.
// This returns nil if not connected or collection is not present in the database.
func Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection {
	return defaultSingleMongoDB.Collection(name, opts...)
}

// SingleMongoDB wraps around a single DB with a single mongodb client.
type SingleMongoDB interface {
	New() SingleMongoDB
	Connect(ctx context.Context, uri, nameDB string, nameCollections ...string) error
	Disconnect(ctx context.Context) error
	Client() *mongo.Client
	Database() *mongo.Database
	Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection
}

// singleMongoDB wraps around a single DB with a single mongodb client.
// Zero value of this is ready to use.
type singleMongoDB struct {
	mu          sync.RWMutex
	client      *mongo.Client
	database    *mongo.Database
	collections map[string]*mongo.Collection
}

func (smdb *singleMongoDB) isConnected() bool {
	return smdb.client != nil
}

// Disconnect from client and delete its object.
// If client is nil then this procedure is noop.
func (smdb *singleMongoDB) disconnect() {
	if smdb.isConnected() {
		for smdb.client.Disconnect(context.Background()) != nil {
		}
		smdb.client = nil
	}
}

// New is a constructor you wouldn't want to use at all.
func (smdb *singleMongoDB) New() SingleMongoDB {
	return &singleMongoDB{}
}

// Connect to a single DB with a single mongo client.
// Errors if it cannot reach any of desired database or collections.
// Call Disconnect method to close down connection.
func (smdb *singleMongoDB) Connect(ctx context.Context, uri, nameDB string, nameCollections ...string) error {
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
	smdb.collections = map[string]*mongo.Collection{}
	for _, nameCollection := range nameCollections {
		collection := smdb.database.Collection(nameCollection)
		if collection == nil {
			smdb.disconnect()
			return fmt.Errorf("cannot get collection: %v", nameCollection)
		}
		smdb.collections[nameCollection] = smdb.database.Collection(nameCollection)
	}
	// return
	return nil
}

// Disconnect the connection to DB.
func (smdb *singleMongoDB) Disconnect(ctx context.Context) error {
	smdb.mu.RLock()
	defer smdb.mu.RUnlock()
	if !smdb.isConnected() {
		return errors.New("not connected")
	}
	return smdb.client.Disconnect(ctx)
}

// Client is a getter returning a mongo client.
// This returns nil if not connected.
func (smdb *singleMongoDB) Client() *mongo.Client {
	smdb.mu.RLock()
	defer smdb.mu.RUnlock()
	if !smdb.isConnected() {
		return nil
	}
	return smdb.client
}

// Database is a getter returning an object representation of a DB.
// This returns nil if not connected.
func (smdb *singleMongoDB) Database() *mongo.Database {
	smdb.mu.RLock()
	defer smdb.mu.RUnlock()
	if !smdb.isConnected() {
		return nil
	}
	return smdb.database
}

// Collection gets a collection from DB efficiently as it uses a map to cache collection objects.
// This returns nil if not connected or collection is not present in the database.
func (smdb *singleMongoDB) Collection(name string, opts ...*options.CollectionOptions) *mongo.Collection {
	smdb.mu.RLock()
	defer smdb.mu.RUnlock()
	if !smdb.isConnected() {
		return nil
	}
	if _, ok := smdb.collections[name]; !ok {
		smdb.collections[name] = smdb.database.Collection(name, opts...)
	}
	return smdb.collections[name]
}
