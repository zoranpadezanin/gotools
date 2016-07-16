package bolt

import (
	"fmt"

	"github.com/boltdb/bolt"
)

//CreateBucket creates a new bucket, or returns error
func CreateBucket(db *bolt.DB, bucket string) error {
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(bucket))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		return nil
	})
	return nil
}
