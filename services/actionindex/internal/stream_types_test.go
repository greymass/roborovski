package internal

import "testing"

func TestActionFilter_Matches_ContractOnly(t *testing.T) {
	contract := uint64(5000)
	action := uint64(6000)
	receiver := uint64(1000)

	filter := ActionFilter{
		Contracts: map[uint64]struct{}{contract: {}},
	}

	tests := []struct {
		name   string
		action StreamedAction
		want   bool
	}{
		{
			name: "receiver equals contract",
			action: StreamedAction{
				Contract: contract,
				Receiver: contract,
				Action:   action,
			},
			want: true,
		},
		{
			name: "receiver differs from contract",
			action: StreamedAction{
				Contract: contract,
				Receiver: receiver,
				Action:   action,
			},
			want: true,
		},
		{
			name: "wrong contract",
			action: StreamedAction{
				Contract: 9999,
				Receiver: receiver,
				Action:   action,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filter.Matches(tt.action)
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestActionFilter_Matches_ContractAndReceiver(t *testing.T) {
	contract := uint64(5000)
	receiver := uint64(1000)
	action := uint64(6000)

	filter := ActionFilter{
		Contracts: map[uint64]struct{}{contract: {}},
		Receivers: map[uint64]struct{}{receiver: {}},
	}

	tests := []struct {
		name   string
		action StreamedAction
		want   bool
	}{
		{
			name: "matches both contract and receiver",
			action: StreamedAction{
				Contract: contract,
				Receiver: receiver,
				Action:   action,
			},
			want: true,
		},
		{
			name: "matches contract but not receiver",
			action: StreamedAction{
				Contract: contract,
				Receiver: 9999,
				Action:   action,
			},
			want: false,
		},
		{
			name: "matches receiver but not contract",
			action: StreamedAction{
				Contract: 9999,
				Receiver: receiver,
				Action:   action,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filter.Matches(tt.action)
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestActionFilter_Matches_ReceiverOnly(t *testing.T) {
	receiver := uint64(1000)
	action := uint64(6000)

	filter := ActionFilter{
		Receivers: map[uint64]struct{}{receiver: {}},
	}

	tests := []struct {
		name   string
		action StreamedAction
		want   bool
	}{
		{
			name: "matches receiver",
			action: StreamedAction{
				Contract: 5000,
				Receiver: receiver,
				Action:   action,
			},
			want: true,
		},
		{
			name: "does not match receiver",
			action: StreamedAction{
				Contract: 5000,
				Receiver: 9999,
				Action:   action,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filter.Matches(tt.action)
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestActionFilter_Matches_WithActions(t *testing.T) {
	contract := uint64(5000)
	action := uint64(6000)

	filter := ActionFilter{
		Contracts: map[uint64]struct{}{contract: {}},
		Actions:   map[uint64]struct{}{action: {}},
	}

	tests := []struct {
		name   string
		action StreamedAction
		want   bool
	}{
		{
			name: "matches contract and action",
			action: StreamedAction{
				Contract: contract,
				Receiver: 1000,
				Action:   action,
			},
			want: true,
		},
		{
			name: "matches contract but not action",
			action: StreamedAction{
				Contract: contract,
				Receiver: 1000,
				Action:   9999,
			},
			want: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := filter.Matches(tt.action)
			if got != tt.want {
				t.Errorf("Matches() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestActionFilter_Matches_EmptyFilter(t *testing.T) {
	filter := ActionFilter{}

	action := StreamedAction{
		Contract: 5000,
		Receiver: 1000,
		Action:   6000,
	}

	if filter.Matches(action) {
		t.Error("Empty filter should not match any action")
	}
}
