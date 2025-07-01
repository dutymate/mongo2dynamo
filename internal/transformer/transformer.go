package transformer

// Transformer provides an interface for transforming documents between formats.
type Transformer interface {
	// Transform returns a new slice of documents with the transformation applied.
	Transform([]map[string]interface{}) ([]map[string]interface{}, error)
}

// MongoToDynamoTransformer transforms MongoDB documents for DynamoDB.
// It renames the '_id' field to 'id' and removes the '__v' and '_class' fields.
type MongoToDynamoTransformer struct{}

// skipFields lists field names to be excluded from the output.
var skipFields = map[string]struct{}{
	"__v":    {},
	"_class": {},
}

// NewMongoToDynamoTransformer returns a new MongoToDynamoTransformer.
// This transformer can be used to convert MongoDB documents to a DynamoDB-compatible format.
func NewMongoToDynamoTransformer() *MongoToDynamoTransformer {
	return &MongoToDynamoTransformer{}
}

// Transform renames the '_id' field to 'id' and removes the '__v' and '_class' fields from each document.
// The '_id' field from MongoDB is mapped to 'id' to match DynamoDB's primary key naming convention.
// The '__v' field, which is often used for versioning in MongoDB/Mongoose, is omitted from the output.
// The '_class' field, which is used by Spring Data for type information, is also omitted from the output.
// The resulting slice contains documents ready to be written to DynamoDB, with no '_id', '__v', or '_class' fields present.
// Returns an error only if an unexpected issue occurs during transformation (none in current implementation).
func (t *MongoToDynamoTransformer) Transform(input []map[string]interface{}) ([]map[string]interface{}, error) {
	output := make([]map[string]interface{}, len(input))
	for i, doc := range input {
		// Calculate the exact number of kept fields (including id).
		kept := 0
		for k := range doc {
			if k == "_id" || skipFields[k] == struct{}{} {
				continue
			}
			kept++
		}
		// Include the id field.
		newDoc := make(map[string]interface{}, kept+1)
		for k, v := range doc {
			if k == "_id" {
				newDoc["id"] = v
				continue
			}
			if _, skip := skipFields[k]; skip {
				continue
			}
			newDoc[k] = v
		}
		output[i] = newDoc
	}
	return output, nil
}
