package ultimate

import (
	"fmt"
	"time"
)

func GenUpdateSQL() string {
	timestamp := time.Now().UTC().UnixNano()
	return fmt.Sprintf("update data set update_data='%s' where id='update'",timestamp)
}
