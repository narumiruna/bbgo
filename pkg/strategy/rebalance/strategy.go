package rebalance

import (
	"context"
	"fmt"
	"math"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/c9s/bbgo/pkg/bbgo"
	"github.com/c9s/bbgo/pkg/fixedpoint"
	"github.com/c9s/bbgo/pkg/types"
)

type TargetOrder struct {
	Symbol   string
	Side     types.SideType
	Quantity fixedpoint.Value
}

func (o TargetOrder) String() string {
	return fmt.Sprintf("TargetOrder{symbol=%v, side=%v, quantity=%v}", o.Symbol, o.Side, o.Quantity)
}

const ID = "rebalance"

var log = logrus.WithField("strategy", ID)

func init() {
	bbgo.RegisterStrategy(ID, &Strategy{})
}

type Strategy struct {
	Notifiability *bbgo.Notifiability

	Interval       types.Interval              `json:"interval"`
	BaseCurrency   string                      `json:"baseCurrency"`
	TargetWeights  map[string]fixedpoint.Value `json:"targetWeights"`
	Threshold      fixedpoint.Value            `json:"threshold"`
	IgnoreLocked   bool                        `json:"ignoreLocked"`
	Verbose        bool                        `json:"verbose"`
	DryRun         bool                        `json:"dryRun"`
	SliceSize      fixedpoint.Value            `json:"sliceSize"`
	UpdateInterval UpdateInterval              `json:"updateInterval"`

	currencies []string
}

func (s *Strategy) Initialize() error {
	for currency := range s.TargetWeights {
		s.currencies = append(s.currencies, currency)
	}

	sort.Strings(s.currencies)
	return nil
}

func (s *Strategy) ID() string {
	return ID
}

func (s *Strategy) Validate() error {
	if len(s.TargetWeights) == 0 {
		return fmt.Errorf("targetWeights should not be empty")
	}

	for currency, weight := range s.TargetWeights {
		if weight.Float64() < 0 {
			return fmt.Errorf("%s weight: %f should not less than 0", currency, weight.Float64())
		}
	}

	if s.Threshold.Sign() < 0 {
		return fmt.Errorf("threshold should not less than 0")
	}

	return nil
}

func (s *Strategy) Subscribe(session *bbgo.ExchangeSession) {
	for _, symbol := range s.getSymbols() {
		session.Subscribe(types.KLineChannel, symbol, types.SubscribeOptions{Interval: s.Interval.String()})
	}
}

func (s *Strategy) Run(ctx context.Context, orderExecutor bbgo.OrderExecutor, session *bbgo.ExchangeSession) error {
	log.Infof("update interval: %v", s.UpdateInterval)
	session.MarketDataStream.OnKLineClosed(func(kline types.KLine) {
		s.rebalance(ctx, orderExecutor, session)
	})
	return nil
}

func (s *Strategy) rebalance(ctx context.Context, orderExecutor bbgo.OrderExecutor, session *bbgo.ExchangeSession) {
	prices, err := s.getPrices(ctx, session)
	if err != nil {
		return
	}

	balances := session.Account.Balances()
	quantities := s.getQuantities(balances)
	marketValues := prices.Mul(quantities)

	targetOrders := s.generateTargetOrders(prices, marketValues)
	for _, order := range targetOrders {
		log.Infof("generated target order: %s", order.String())
	}

	if s.DryRun {
		return
	}

	s.executeTargetOrders(targetOrders, session, ctx)
}

func (s *Strategy) executeTargetOrders(targetOrders []TargetOrder, session *bbgo.ExchangeSession, ctx context.Context) {
	weightGroup := new(sync.WaitGroup)
	weightGroup.Add(len(targetOrders))
	for _, o := range targetOrders {
		go s.executeTwap(session, o, ctx, weightGroup)
	}
	weightGroup.Wait()
}

func (s *Strategy) executeTwap(session *bbgo.ExchangeSession, o TargetOrder, ctx context.Context, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	sliceQuantity := o.Quantity.Mul(s.SliceSize)
	execution := &bbgo.TwapExecution{
		Session:        session,
		Symbol:         o.Symbol,
		Side:           o.Side,
		TargetQuantity: o.Quantity,
		SliceQuantity:  sliceQuantity,
		UpdateInterval: time.Duration(s.UpdateInterval),
	}
	log.Infof("twap execution, symbol: %v, target quantity: %v, slice quantity: %v", o.Symbol, o.Quantity.Float64(), sliceQuantity.Float64())

	executionCtx, cancelExecution := context.WithCancel(ctx)
	defer cancelExecution()
	if err := execution.Run(executionCtx); err != nil {
		log.WithError(err)
		return
	}

	var sigC = make(chan os.Signal, 1)
	signal.Notify(sigC, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigC)

	select {
	case sig := <-sigC:
		log.Warnf("signal %v", sig)
		log.Infof("shutting down order executor...")
		shutdownCtx, cancelShutdown := context.WithDeadline(ctx, time.Now().Add(10*time.Second))
		execution.Shutdown(shutdownCtx)
		cancelShutdown()

	case <-execution.Done():
		log.Infof("the order execution is completed")

	case <-ctx.Done():
	}
}

func (s *Strategy) getPrices(ctx context.Context, session *bbgo.ExchangeSession) (prices types.Float64Slice, err error) {
	for _, currency := range s.currencies {
		if currency == s.BaseCurrency {
			prices = append(prices, 1.0)
			continue
		}

		symbol := currency + s.BaseCurrency
		ticker, err := session.Exchange.QueryTicker(ctx, symbol)
		if err != nil {
			s.Notifiability.Notify("query ticker error: %s", err.Error())
			log.WithError(err).Error("query ticker error")
			return prices, err
		}

		prices = append(prices, ticker.Last.Float64())
	}
	return prices, nil
}

func (s *Strategy) getQuantities(balances types.BalanceMap) (quantities types.Float64Slice) {
	for _, currency := range s.currencies {
		if s.IgnoreLocked {
			quantities = append(quantities, balances[currency].Total().Float64())
		} else {
			quantities = append(quantities, balances[currency].Available.Float64())
		}
	}
	return quantities
}

func (s *Strategy) generateTargetOrders(prices, marketValues types.Float64Slice) (targetOrders []TargetOrder) {
	currentWeights := marketValues.Normalize()
	totalValue := marketValues.Sum()

	log.Infof("total value: %f", totalValue)

	for i, currency := range s.currencies {
		if currency == s.BaseCurrency {
			continue
		}

		symbol := currency + s.BaseCurrency
		currentWeight := currentWeights[i]
		currentPrice := prices[i]
		targetWeight := s.TargetWeights[currency].Float64()

		log.Infof("%s price: %.2f, current weight: %.2f, target weight: %.2f",
			symbol,
			currentPrice,
			currentWeight,
			targetWeight)

		// calculate the difference between current weight and target weight
		// if the difference is less than threshold, then we will not create the order
		weightDifference := targetWeight - currentWeight
		if math.Abs(weightDifference) < s.Threshold.Float64() {
			log.Infof("%s weight distance |%.2f - %.2f| = |%.2f| less than the threshold: %.2f",
				symbol,
				currentWeight,
				targetWeight,
				weightDifference,
				s.Threshold.Float64())
			continue
		}

		quantity := fixedpoint.NewFromFloat((weightDifference * totalValue) / currentPrice)

		side := types.SideTypeBuy
		if quantity.Sign() < 0 {
			side = types.SideTypeSell
			quantity = quantity.Abs()
		}

		log.Debugf("symbol: %v, quantity: %v", symbol, quantity)

		targetOrder := TargetOrder{
			Symbol:   symbol,
			Side:     side,
			Quantity: quantity,
		}
		targetOrders = append(targetOrders, targetOrder)
	}
	return targetOrders
}

func (s *Strategy) getSymbols() (symbols []string) {
	for _, currency := range s.currencies {
		if currency == s.BaseCurrency {
			continue
		}
		symbols = append(symbols, currency+s.BaseCurrency)
	}
	return symbols
}
