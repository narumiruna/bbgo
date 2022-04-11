package rebalance

import (
	"encoding/json"
	"time"
)

type UpdateInterval time.Duration

func (i UpdateInterval) String() string {
	return time.Duration(i).String()
}

func (i *UpdateInterval) UnmarshalJSON(b []byte) (err error) {
	var a string
	err = json.Unmarshal(b, &a)
	if err != nil {
		return err
	}

	d, err := time.ParseDuration(a)
	if err != nil {
		return err
	}

	*i = UpdateInterval(d)
	return
}
