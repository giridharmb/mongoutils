package mongoutils

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.uber.org/zap"
)

/*
MongoConnectionURL ...
*/
var MongoConnectionURL = "mongodb://my-mongodb-server"

/*
Logger ...
*/
var Logger *zap.Logger

/*
GenericData ...
*/
type GenericData struct {
	Key   string
	Value interface{}
}

/*
Initialize ...
*/
func Initialize(mongoURL string) {
	MongoConnectionURL = mongoURL
	Logger, _ = zap.NewDevelopment()
}

/*
InsertGenericIfNotExists ...
*/
func InsertGenericIfNotExists(data GenericData, db string, coll string) (resultStatus map[string]string, err error) {

	debugMessage := ""

	defer func() {
		if r := recover(); r != nil {
			debugMessage := fmt.Sprintf("InsertGenericIfNotExists : Recovered in f : %v", r)
			Logger.Debug(debugMessage)
			// find out exactly what the error was and set err
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("defer : unknown panic in InsertGenericIfNotExists")
			}
		}
	}()

	// Set client options
	clientOptions := options.Client().ApplyURI(MongoConnectionURL)

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err = client.Disconnect(context.TODO())
		if err != nil {
			log.Fatal(err)
		} else {
			Logger.Debug("Connection to MongoDB closed.")
		}
	}()

	Logger.Debug("Connected to MongoDB!")

	// Get a handle for your collection
	database := client.Database(db)
	collection := database.Collection(coll)

	errorMessage := ""

	resultStatus = make(map[string]string)
	recordKey := data.Key
	filter := bson.D{{Key: "key", Value: recordKey}}
	update := bson.D{{Key: "$set", Value: data}}

	updateResult, err := collection.UpdateOne(context.TODO(), filter, update, options.Update().SetUpsert(true))
	if err != nil {
		errorMessage = fmt.Sprintf("ERROR : InsertGenericIfNotExists() : Could not update  document with key (%v) : %v", recordKey, err.Error())
		Logger.Debug(errorMessage)
		resultStatus["insertion"] = "failed"
		resultStatus["status"] = ""
		resultStatus["error"] = fmt.Sprintf("%v", errorMessage)
		return resultStatus, nil
	}
	debugMessage = fmt.Sprintf("InsertGenericIfNotExists() : Matched %v documents and updated %v documents.", updateResult.MatchedCount, updateResult.ModifiedCount)
	fmt.Printf("\n%v\n", debugMessage)
	resultStatus["insertion"] = "successful"
	resultStatus["status"] = debugMessage
	resultStatus["error"] = "nil"
	return resultStatus, nil
}

/*
DeleteGenericIfExists ...
*/
func DeleteGenericIfExists(key string, db string, coll string) (resultStatus map[string]string, err error) {

	debugMessage := ""

	defer func() {
		if r := recover(); r != nil {

			debugMessage := fmt.Sprintf("DeleteGenericIfExists : Recovered in f : %v", r)
			Logger.Debug(debugMessage)

			// find out exactly what the error was and set err
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("defer : unknown panic in DeleteGenericIfExists")
			}
		}
	}()

	// Set client options
	clientOptions := options.Client().ApplyURI(MongoConnectionURL)

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err = client.Disconnect(context.TODO())
		if err != nil {
			log.Fatal(err)
		} else {
			Logger.Debug("Connection to MongoDB closed.")
		}
	}()

	Logger.Debug("Connected to MongoDB!")

	// Get a handle for your collection
	database := client.Database(db)
	collection := database.Collection(coll)

	errorMessage := ""

	resultStatus = make(map[string]string)
	//Delete one
	deleteResult, err := collection.DeleteOne(context.TODO(), bson.D{{Key: "key", Value: key}})
	if err != nil {
		errorMessage = fmt.Sprintf("ERROR : deleteGenericIfExists() : Could not delete document with key (%v) : %v", key, err.Error())
		Logger.Debug(errorMessage)
		resultStatus["deletion"] = "failed"
		resultStatus["error"] = fmt.Sprintf("%v", errorMessage)
		return resultStatus, nil
	}
	if deleteResult.DeletedCount == 0 {
		resultStatus["deletion"] = "failed"
		resultStatus["error"] = "document_not_found"
		return resultStatus, nil
	}
	debugMessage = fmt.Sprintf("Deleted (%v) # of documents", deleteResult.DeletedCount)
	fmt.Printf("\n%v\n", debugMessage)
	debugMessage = fmt.Sprintf("deleteGenericIfExists() : Deleted Document with key (%v)", key)
	fmt.Printf("\n%v\n", debugMessage)
	resultStatus["deletion"] = "successful"
	resultStatus["error"] = "nil"
	return resultStatus, nil
}

/*
FetchCollection ...
*/
func FetchCollection(db string, coll string) (results []interface{}, err error) {
	debugMessage := ""

	defer func() {
		if r := recover(); r != nil {
			debugMessage = fmt.Sprintf("FetchCollection : Recovered in f : %v", r)
			Logger.Debug(debugMessage)

			// find out exactly what the error was and set err
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("defer : unknown panic in FetchCollection")
			}
		}
	}()

	// Set client options
	clientOptions := options.Client().ApplyURI(MongoConnectionURL)

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err = client.Disconnect(context.TODO())
		if err != nil {
			log.Fatal(err)
		} else {
			Logger.Debug("Connection to MongoDB closed.")
		}
	}()

	Logger.Debug("Connected to MongoDB!")

	// Get a handle for your collection
	database := client.Database(db)
	collection := database.Collection(coll)

	errorMessage := ""

	results = make([]interface{}, 0)
	//Delete one
	cursor, err := collection.Find(context.TODO(), bson.M{})
	if err != nil {
		errorMessage = fmt.Sprintf("ERROR : FetchCollection() : Could not fetch data from entire collection : %v", err.Error())
		Logger.Debug(errorMessage)
		return results, errors.New(errorMessage)
	}

	defer func() {
		_ = cursor.Close(context.TODO())
	}()

	for cursor.Next(context.TODO()) {
		var genericData bson.M
		if err = cursor.Decode(&genericData); err != nil {
			return results, errors.New(errorMessage)
		}
		results = append(results, genericData)
	}
	return results, nil
}

/*
DeleteManyIfExists ...
*/
func DeleteManyIfExists(keysToDelete []string, db string, coll string) (result []map[string]string, err error) {
	debugMessage := ""

	defer func() {
		if r := recover(); r != nil {
			debugMessage = fmt.Sprintf("DeleteManyIfExists : Recovered in f : %v", r)
			Logger.Debug(debugMessage)

			// find out exactly what the error was and set err
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("defer : unknown panic in DeleteManyIfExists")
			}
		}
	}()

	// Set client options
	clientOptions := options.Client().ApplyURI(MongoConnectionURL)

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err = client.Disconnect(context.TODO())
		if err != nil {
			log.Fatal(err)
		} else {
			Logger.Debug("Connection to MongoDB closed.")
		}
	}()

	Logger.Debug("Connected to MongoDB!")

	// Get a handle for your collection
	//database := client.Database(db)
	//collection := database.Collection(coll)

	errorMessage := ""
	result = make([]map[string]string, 0)

	for _, key := range keysToDelete {
		//deleteGenericIfExists(key string, collection *mongo.Collection) (resultStatus map[string]string, err error)
		status := make(map[string]string)
		recordKey := key
		deletionResult, err := DeleteGenericIfExists(recordKey, db, coll)
		if err != nil {
			errorMessage = fmt.Sprintf("ERROR : DeleteManyIfExists() : Could not delete record with key (%v) : %v", recordKey, err.Error())
			Logger.Debug(errorMessage)
			status[recordKey] = "failed"
			status["error"] = fmt.Sprintf(errorMessage)
		} else {
			if deletionResult["deletion"] == "successful" {
				status[recordKey] = "successful"
				status["error"] = "nil"
			} else {
				status[recordKey] = "failed"
				status["error"] = deletionResult["error"]
			}
		}
		result = append(result, status)
	}
	return result, nil
}

/*
InsertManyIfNotExists ...
*/
func InsertManyIfNotExists(kvList []GenericData, db string, coll string) (resultStatus []map[string]string, err error) {
	debugMessage := ""
	defer func() {
		if r := recover(); r != nil {
			debugMessage = fmt.Sprintf("InsertManyIfNotExists : Recovered in f : %v", r)
			Logger.Debug(debugMessage)
			// find out exactly what the error was and set err
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("defer : unknown panic in InsertManyIfNotExists")
			}
		}
	}()

	// Set client options
	clientOptions := options.Client().ApplyURI(MongoConnectionURL)

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err = client.Disconnect(context.TODO())
		if err != nil {
			log.Fatal(err)
		} else {
			Logger.Debug("Connection to MongoDB closed.")
		}
	}()

	Logger.Debug("Connected to MongoDB!")

	// Get a handle for your collection
	database := client.Database(db)
	collection := database.Collection(coll)

	errorMessage := ""

	resultStatus = make([]map[string]string, 0)

	for _, kv := range kvList {
		status := make(map[string]string)
		recordKey := kv.Key
		filter := bson.D{{Key: "key", Value: recordKey}}
		update := bson.D{{Key: "$set", Value: kv}}
		updateResult, err := collection.UpdateOne(context.TODO(), filter, update, options.Update().SetUpsert(true))
		if err != nil {
			errorMessage = fmt.Sprintf("ERROR : InsertManyIfNotExists() : Could not update  document with key (%v) : %v", recordKey, err.Error())
			Logger.Debug(errorMessage)
			status[kv.Key] = "failed"
			status["error"] = fmt.Sprintf(errorMessage)
		} else {
			status[kv.Key] = "successful"
			status["error"] = "nil"
			debugMessage = fmt.Sprintf("InsertManyIfNotExists() : Matched %v documents and updated %v documents.", updateResult.MatchedCount, updateResult.ModifiedCount)
			fmt.Printf("\n%v\n", debugMessage)
		}
		resultStatus = append(resultStatus, status)
	}
	return resultStatus, nil
}

/*
FindGenericRecord ...
*/
func FindGenericRecord(key string, db string, coll string) (seachedRecord map[string]interface{}, err error) {
	debugMessage := ""
	defer func() {
		if r := recover(); r != nil {
			debugMessage = fmt.Sprintf("FindGenericRecord : Recovered in f : %v", r)
			Logger.Debug(debugMessage)
			// find out exactly what the error was and set err
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("defer : unknown panic in FindGenericRecord")
			}
		}
	}()

	// Set client options
	clientOptions := options.Client().ApplyURI(MongoConnectionURL)

	// Connect to MongoDB
	client, err := mongo.Connect(context.TODO(), clientOptions)
	if err != nil {
		log.Fatal(err)
	}

	// Check the connection
	err = client.Ping(context.TODO(), nil)
	if err != nil {
		log.Fatal(err)
	}

	defer func() {
		err = client.Disconnect(context.TODO())
		if err != nil {
			log.Fatal(err)
		} else {
			Logger.Debug("\nConnection to MongoDB closed.")
		}
	}()

	Logger.Debug("Connected to MongoDB!")

	// Get a handle for your collection
	database := client.Database(db)
	collection := database.Collection(coll)

	errorMessage := ""

	seachedRecord = make(map[string]interface{})

	var result GenericData
	filter := bson.D{{Key: "key", Value: key}}
	err = collection.FindOne(context.TODO(), filter).Decode(&result)
	if err != nil {
		errorMessage = fmt.Sprintf("FindGenericRecord() : Could not find document with key (%v) : %v", key, err.Error())
		Logger.Debug(errorMessage)
		seachedRecord["search_result"] = "not_found"
		seachedRecord["key"] = key
		seachedRecord["value"] = nil
		seachedRecord["error"] = fmt.Sprintf("%v", errorMessage)
		return seachedRecord, nil
	}
	debugMessage = fmt.Sprintf("FindGenericRecord() : Found a single document: %+v", result)
	fmt.Printf("\n%v\n", debugMessage)
	seachedRecord["search_result"] = "found"
	seachedRecord["key"] = key
	seachedRecord["error"] = ""
	seachedRecord["value"] = result
	return seachedRecord, nil
}

/*
FindMultipleGenericRecord ...
*/
func FindMultipleGenericRecord(keys []string, db string, coll string) (seachedRecords []map[string]interface{}, err error) {
	debugMessage := ""

	defer func() {
		if r := recover(); r != nil {
			debugMessage = fmt.Sprintf("FindMultipleGenericRecord : Recovered in f : %v", r)
			Logger.Debug(debugMessage)
			// find out exactly what the error was and set err
			switch x := r.(type) {
			case string:
				err = errors.New(x)
			case error:
				err = x
			default:
				err = errors.New("defer : unknown panic in FindMultipleGenericRecord")
			}
		}
	}()

	maxNumberOfWorkers := 10

	seachedRecords = make([]map[string]interface{}, 0)

	jobs := make(chan map[string]string, 1)

	results := make(chan map[string]interface{}, 1)

	var wg sync.WaitGroup

	jobList := make([]map[string]string, 0)

	for _, key := range keys {
		myMap := make(map[string]string)
		myMap["key"] = key
		myMap["coll"] = coll
		myMap["db"] = db
		jobList = append(jobList, myMap)
	}

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for w := 1; w <= maxNumberOfWorkers; w++ {
			go findWorker(jobs, results)
		}
		fmt.Printf("\n@ Putting data into jobs...\n")
		for _, job := range jobList {
			jobs <- job
		}
		fmt.Printf("\n@ jobs channel got all data !\n")
		close(jobs)
		fmt.Printf("\n@ jobs channel closed !\n")
	}(&wg)

	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 1; i <= len(keys); i++ {
			x := <-results
			seachedRecords = append(seachedRecords, x)
		}
	}(&wg)

	wg.Wait()

	return seachedRecords, nil
}

func findWorker(jobs <-chan map[string]string, results chan<- map[string]interface{}) {
	for job := range jobs {
		key := job["key"]
		db := job["db"]
		coll := job["coll"]
		seachedRecord, _ := FindGenericRecord(key, db, coll)
		results <- seachedRecord
	}
}
