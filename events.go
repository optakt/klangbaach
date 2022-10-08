package main

import (
	"math/big"

	"github.com/ethereum/go-ethereum/crypto"
)

const (
	EventSwap = "Swap(address,uint256,uint256,uint256,uint256,address)"
	EventSync = "Sync(uint112,uint112)"
)

var (
	SigSwap = crypto.Keccak256Hash([]byte(EventSwap))
	SigSync = crypto.Keccak256Hash([]byte(EventSync))
)

type Swap struct {
	Amount0In  *big.Int
	Amount1In  *big.Int
	Amount0Out *big.Int
	Amount1Out *big.Int
}

type Sync struct {
	Reserve0 *big.Int
	Reserve1 *big.Int
}
