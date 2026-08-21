package investgo

import "time"

type HistoricCandle struct {
	Uid      string    `ch:"uid"`
	Interval int32     `ch:"interval"`
	Time     time.Time `ch:"time"`
	Open     float64   `ch:"open"`
	High     float64   `ch:"high"`
	Low      float64   `ch:"low"`
	Close    float64   `ch:"close"`
	Volume   int64     `ch:"volume"`
}
