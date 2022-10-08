package main

import (
	"github.com/ethereum/go-ethereum/crypto"
)

var (
	SigBurn = crypto.Keccak256Hash([]byte(EventBurn))
	SigMint = crypto.Keccak256Hash([]byte(EventMint))
	SigSwap = crypto.Keccak256Hash([]byte(EventSwap))
	SigSync = crypto.Keccak256Hash([]byte(EventSync))
)
