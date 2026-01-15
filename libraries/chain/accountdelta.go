package chain

type AccountDelta struct {
	Account string `json:"account"`
	Delta   int64  `json:"delta"` // Positive = RAM increase, Negative = RAM decrease
}
