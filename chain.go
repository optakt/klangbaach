package main

type Chain struct {
	Name      string `json:"name"`
	ChainID   uint64 `json:"chainId"`
	ShortName string `json:"shortName"`
	NetworID  uint64 `json:"networkId"`
}
