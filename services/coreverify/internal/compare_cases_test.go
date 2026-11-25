package internal

import (
	"testing"

	"github.com/greymass/roborovski/libraries/chain"
)

func TestCountActionSystemCases(t *testing.T) {
	cases := []struct {
		name     string
		receiver string
		action   string
		want     string
		count    int
	}{
		{"onerror delivered to the deferred sender", "prospectorsp", "onerror", CaseOnerrorActions, 1},
		{"setabi on eosio", "eosio", "setabi", CaseSetabiActions, 1},
		{"setabi notification", "abiwatcher", "setabi", CaseSetabiActions, 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			counts := map[string]int{}
			countAction(&chain.ActionTrace{
				Receiver: tc.receiver,
				Act: chain.Action{
					Account:       "eosio",
					Name:          tc.action,
					Authorization: []chain.PermissionLevel{{Actor: tc.receiver, Permission: "active"}},
					Data:          "00",
				},
			}, counts)
			if counts[tc.want] != tc.count {
				t.Fatalf("%s = %d, want %d", tc.want, counts[tc.want], tc.count)
			}
		})
	}
}
