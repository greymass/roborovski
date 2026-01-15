package fcraw

type BlockTraceV2 struct {
	ID                  [32]byte
	Number              uint32
	PreviousID          [32]byte
	Timestamp           uint32
	Producer            uint64
	TransactionMroot    [32]byte
	ActionMroot         [32]byte
	ScheduleVersion     uint32
	TransactionsVariant uint32
	TransactionsV2      []TransactionTraceV2
	TransactionsV3      []TransactionTraceV3
}

type TransactionTraceV2 struct {
	ID             [32]byte
	ActionsVariant uint32
	Actions        []ActionTraceV1
	Status         uint8
	CpuUsageUs     uint32
	NetUsageWords  uint32
	Signatures     []*Signature
	TrxHeader      TransactionHeader
}

type TransactionTraceV3 struct {
	TransactionTraceV2
	BlockNum        uint32
	BlockTime       uint32
	ProducerBlockID *[32]byte
}

type ActionTraceV1 struct {
	ActionTrace
	ReturnValue []byte
}

type ActionTrace struct {
	ActionOrdinal                          uint32
	CreatorActionOrdinal                   uint32
	ClosestUnnotifiedAncestorActionOrdinal uint32
	ReceiptReceiver                        uint64
	GlobalSequence                         uint64
	RecvSequence                           uint64
	AuthSequence                           map[uint64]uint64
	CodeSequence                           uint32
	AbiSequence                            uint32
	Receiver                               uint64
	Account                                uint64
	Name                                   uint64
	Authorization                          []AuthorizationTrace
	Data                                   []byte
	ContextFree                            bool
	Elapsed                                int64
	AccountRamDeltas                       []AccountDelta
}

type AuthorizationTrace struct {
	Account    uint64
	Permission uint64
}

type AccountDelta struct {
	Account uint64
	Delta   int64
}

type Signature struct {
	Type     uint32
	Data     []byte
	WebAuthn *WebAuthnSignature
}

type WebAuthnSignature struct {
	Signature  []byte
	AuthData   []byte
	ClientData []byte
}

type TransactionHeader struct {
	Expiration       uint32
	RefBlockNum      uint16
	RefBlockPrefix   uint32
	MaxNetUsageWords uint32
	MaxCpuUsageMs    uint8
	DelaySec         uint32
}
