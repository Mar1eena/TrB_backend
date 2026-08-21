package server

import (
	"context"

	pb "opensource.tbank.ru/invest/invest-go/proto"
)

func (s *InstrumentsService) TradingSchedules(ctx context.Context, req *pb.TradingSchedulesRequest) (*pb.TradingSchedulesResponse, error) {
	if req == nil {
		req = &pb.TradingSchedulesRequest{}
	}
	r, err := s.client.TradingSchedules(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить расписания торговых площадок")
		return nil, err
	}
	s.log.Info().Int("exchanges", len(r.GetExchanges())).Msg("расписания торговых площадок получены")
	return r, nil
}

func (s *InstrumentsService) BondBy(ctx context.Context, req *pb.InstrumentRequest) (*pb.BondResponse, error) {
	if req == nil {
		req = &pb.InstrumentRequest{}
	}
	r, err := s.client.BondBy(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("id", req.GetId()).Msg("не удалось получить облигацию")
		return nil, err
	}
	s.log.Info().Str("ticker", r.GetInstrument().GetTicker()).Str("figi", r.GetInstrument().GetFigi()).Msg("облигация получена")
	return r, nil
}

func (s *InstrumentsService) Bonds(ctx context.Context, req *pb.InstrumentsRequest) (*pb.BondsResponse, error) {
	if req == nil {
		req = &pb.InstrumentsRequest{}
	}
	r, err := s.client.Bonds(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить список облигаций")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetInstruments())).Msg("список облигаций получен")
	return r, nil
}

func (s *InstrumentsService) GetBondCoupons(ctx context.Context, req *pb.GetBondCouponsRequest) (*pb.GetBondCouponsResponse, error) {
	if req == nil {
		req = &pb.GetBondCouponsRequest{}
	}
	r, err := s.client.GetBondCoupons(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("figi", req.GetFigi()).Str("instrument_id", req.GetInstrumentId()).Msg("не удалось получить купоны по облигации")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetEvents())).Msg("купоны по облигации получены")
	return r, nil
}

func (s *InstrumentsService) GetBondEvents(ctx context.Context, req *pb.GetBondEventsRequest) (*pb.GetBondEventsResponse, error) {
	if req == nil {
		req = &pb.GetBondEventsRequest{}
	}
	r, err := s.client.GetBondEvents(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("instrument_id", req.GetInstrumentId()).Msg("не удалось получить события по облигации")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetEvents())).Msg("события по облигации получены")
	return r, nil
}

func (s *InstrumentsService) CurrencyBy(ctx context.Context, req *pb.InstrumentRequest) (*pb.CurrencyResponse, error) {
	if req == nil {
		req = &pb.InstrumentRequest{}
	}
	r, err := s.client.CurrencyBy(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("id", req.GetId()).Msg("не удалось получить валюту")
		return nil, err
	}
	s.log.Info().Str("ticker", r.GetInstrument().GetTicker()).Str("figi", r.GetInstrument().GetFigi()).Msg("валюта получена")
	return r, nil
}

func (s *InstrumentsService) Currencies(ctx context.Context, req *pb.InstrumentsRequest) (*pb.CurrenciesResponse, error) {
	if req == nil {
		req = &pb.InstrumentsRequest{}
	}
	r, err := s.client.Currencies(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить список валют")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetInstruments())).Msg("список валют получен")
	return r, nil
}

func (s *InstrumentsService) EtfBy(ctx context.Context, req *pb.InstrumentRequest) (*pb.EtfResponse, error) {
	if req == nil {
		req = &pb.InstrumentRequest{}
	}
	r, err := s.client.EtfBy(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("id", req.GetId()).Msg("не удалось получить ETF")
		return nil, err
	}
	s.log.Info().Str("ticker", r.GetInstrument().GetTicker()).Str("figi", r.GetInstrument().GetFigi()).Msg("ETF получен")
	return r, nil
}

func (s *InstrumentsService) Etfs(ctx context.Context, req *pb.InstrumentsRequest) (*pb.EtfsResponse, error) {
	if req == nil {
		req = &pb.InstrumentsRequest{}
	}
	r, err := s.client.Etfs(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить список ETF")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetInstruments())).Msg("список ETF получен")
	return r, nil
}

func (s *InstrumentsService) FutureBy(ctx context.Context, req *pb.InstrumentRequest) (*pb.FutureResponse, error) {
	if req == nil {
		req = &pb.InstrumentRequest{}
	}
	r, err := s.client.FutureBy(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("id", req.GetId()).Msg("не удалось получить фьючерс")
		return nil, err
	}
	s.log.Info().Str("ticker", r.GetInstrument().GetTicker()).Str("figi", r.GetInstrument().GetFigi()).Msg("фьючерс получен")
	return r, nil
}

func (s *InstrumentsService) Futures(ctx context.Context, req *pb.InstrumentsRequest) (*pb.FuturesResponse, error) {
	if req == nil {
		req = &pb.InstrumentsRequest{}
	}
	r, err := s.client.Futures(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить список фьючерсов")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetInstruments())).Msg("список фьючерсов получен")
	return r, nil
}

func (s *InstrumentsService) OptionBy(ctx context.Context, req *pb.InstrumentRequest) (*pb.OptionResponse, error) {
	if req == nil {
		req = &pb.InstrumentRequest{}
	}
	r, err := s.client.OptionBy(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("id", req.GetId()).Msg("не удалось получить опцион")
		return nil, err
	}
	s.log.Info().Str("ticker", r.GetInstrument().GetTicker()).Str("name", r.GetInstrument().GetName()).Msg("опцион получен")
	return r, nil
}

func (s *InstrumentsService) Options(ctx context.Context, req *pb.InstrumentsRequest) (*pb.OptionsResponse, error) {
	if req == nil {
		req = &pb.InstrumentsRequest{}
	}
	r, err := s.client.Options(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить список опционов")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetInstruments())).Msg("список опционов получен")
	return r, nil
}

func (s *InstrumentsService) OptionsBy(ctx context.Context, req *pb.FilterOptionsRequest) (*pb.OptionsResponse, error) {
	if req == nil {
		req = &pb.FilterOptionsRequest{}
	}
	r, err := s.client.OptionsBy(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("basic_asset_uid", req.GetBasicAssetUid()).Msg("не удалось отфильтровать опционы")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetInstruments())).Msg("опционы отфильтрованы")
	return r, nil
}

func (s *InstrumentsService) ShareBy(ctx context.Context, req *pb.InstrumentRequest) (*pb.ShareResponse, error) {
	if req == nil {
		req = &pb.InstrumentRequest{}
	}
	r, err := s.client.ShareBy(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("id", req.GetId()).Msg("не удалось получить акцию")
		return nil, err
	}
	s.log.Info().Str("ticker", r.GetInstrument().GetTicker()).Str("figi", r.GetInstrument().GetFigi()).Msg("акция получена")
	return r, nil
}

func (s *InstrumentsService) Shares(ctx context.Context, req *pb.InstrumentsRequest) (*pb.SharesResponse, error) {
	if req == nil {
		req = &pb.InstrumentsRequest{}
	}
	r, err := s.client.Shares(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить список акций")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetInstruments())).Msg("список акций получен")
	return r, nil
}

func (s *InstrumentsService) DfaBy(ctx context.Context, req *pb.InstrumentRequest) (*pb.DfaResponse, error) {
	if req == nil {
		req = &pb.InstrumentRequest{}
	}
	r, err := s.client.DfaBy(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("id", req.GetId()).Msg("не удалось получить ЦФА")
		return nil, err
	}
	s.log.Info().Str("ticker", r.GetTicker()).Str("name", r.GetName()).Msg("ЦФА получен")
	return r, nil
}

func (s *InstrumentsService) Dfas(ctx context.Context, req *pb.DfasRequest) (*pb.DfasResponse, error) {
	if req == nil {
		req = &pb.DfasRequest{}
	}
	r, err := s.client.Dfas(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить список ЦФА")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetInstruments())).Msg("список ЦФА получен")
	return r, nil
}

func (s *InstrumentsService) Indicatives(ctx context.Context, req *pb.IndicativesRequest) (*pb.IndicativesResponse, error) {
	if req == nil {
		req = &pb.IndicativesRequest{}
	}
	r, err := s.client.Indicatives(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить индикативные инструменты")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetInstruments())).Msg("индикативные инструменты получены")
	return r, nil
}

func (s *InstrumentsService) GetAccruedInterests(ctx context.Context, req *pb.GetAccruedInterestsRequest) (*pb.GetAccruedInterestsResponse, error) {
	if req == nil {
		req = &pb.GetAccruedInterestsRequest{}
	}
	r, err := s.client.GetAccruedInterests(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("figi", req.GetFigi()).Str("instrument_id", req.GetInstrumentId()).Msg("не удалось получить НКД по облигации")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetAccruedInterests())).Msg("НКД по облигации получен")
	return r, nil
}

func (s *InstrumentsService) GetFuturesMargin(ctx context.Context, req *pb.GetFuturesMarginRequest) (*pb.GetFuturesMarginResponse, error) {
	if req == nil {
		req = &pb.GetFuturesMarginRequest{}
	}
	r, err := s.client.GetFuturesMargin(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("figi", req.GetFigi()).Str("instrument_id", req.GetInstrumentId()).Msg("не удалось получить ГО по фьючерсу")
		return nil, err
	}
	s.log.Info().Msg("ГО по фьючерсу получено")
	return r, nil
}

func (s *InstrumentsService) GetInstrumentBy(ctx context.Context, req *pb.InstrumentRequest) (*pb.InstrumentResponse, error) {
	if req == nil {
		req = &pb.InstrumentRequest{}
	}
	r, err := s.client.GetInstrumentBy(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("id", req.GetId()).Msg("не удалось получить инструмент")
		return nil, err
	}
	s.log.Info().Str("ticker", r.GetInstrument().GetTicker()).Str("name", r.GetInstrument().GetName()).Msg("инструмент получен")
	return r, nil
}

func (s *InstrumentsService) GetDividends(ctx context.Context, req *pb.GetDividendsRequest) (*pb.GetDividendsResponse, error) {
	if req == nil {
		req = &pb.GetDividendsRequest{}
	}
	r, err := s.client.GetDividends(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("figi", req.GetFigi()).Str("instrument_id", req.GetInstrumentId()).Msg("не удалось получить дивиденды")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetDividends())).Msg("дивиденды получены")
	return r, nil
}

func (s *InstrumentsService) GetAssetBy(ctx context.Context, req *pb.AssetRequest) (*pb.AssetResponse, error) {
	if req == nil {
		req = &pb.AssetRequest{}
	}
	r, err := s.client.GetAssetBy(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("id", req.GetId()).Msg("не удалось получить актив")
		return nil, err
	}
	s.log.Info().Str("name", r.GetAsset().GetName()).Msg("актив получен")
	return r, nil
}

func (s *InstrumentsService) GetAssets(ctx context.Context, req *pb.AssetsRequest) (*pb.AssetsResponse, error) {
	if req == nil {
		req = &pb.AssetsRequest{}
	}
	r, err := s.client.GetAssets(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить список активов")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetAssets())).Msg("список активов получен")
	return r, nil
}

func (s *InstrumentsService) GetFavorites(ctx context.Context, req *pb.GetFavoritesRequest) (*pb.GetFavoritesResponse, error) {
	if req == nil {
		req = &pb.GetFavoritesRequest{}
	}
	r, err := s.client.GetFavorites(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить избранные инструменты")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetFavoriteInstruments())).Msg("избранные инструменты получены")
	return r, nil
}

func (s *InstrumentsService) EditFavorites(ctx context.Context, req *pb.EditFavoritesRequest) (*pb.EditFavoritesResponse, error) {
	if req == nil {
		req = &pb.EditFavoritesRequest{}
	}
	r, err := s.client.EditFavorites(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось отредактировать избранные инструменты")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetFavoriteInstruments())).Msg("избранные инструменты обновлены")
	return r, nil
}

func (s *InstrumentsService) CreateFavoriteGroup(ctx context.Context, req *pb.CreateFavoriteGroupRequest) (*pb.CreateFavoriteGroupResponse, error) {
	if req == nil {
		req = &pb.CreateFavoriteGroupRequest{}
	}
	r, err := s.client.CreateFavoriteGroup(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("group_name", req.GetGroupName()).Msg("не удалось создать группу избранного")
		return nil, err
	}
	s.log.Info().Str("group_id", r.GetGroupId()).Str("group_name", r.GetGroupName()).Msg("группа избранного создана")
	return r, nil
}

func (s *InstrumentsService) DeleteFavoriteGroup(ctx context.Context, req *pb.DeleteFavoriteGroupRequest) (*pb.DeleteFavoriteGroupResponse, error) {
	if req == nil {
		req = &pb.DeleteFavoriteGroupRequest{}
	}
	r, err := s.client.DeleteFavoriteGroup(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("group_id", req.GetGroupId()).Msg("не удалось удалить группу избранного")
		return nil, err
	}
	s.log.Info().Msg("группа избранного удалена")
	return r, nil
}

func (s *InstrumentsService) GetFavoriteGroups(ctx context.Context, req *pb.GetFavoriteGroupsRequest) (*pb.GetFavoriteGroupsResponse, error) {
	if req == nil {
		req = &pb.GetFavoriteGroupsRequest{}
	}
	r, err := s.client.GetFavoriteGroups(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить группы избранного")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetGroups())).Msg("группы избранного получены")
	return r, nil
}

func (s *InstrumentsService) GetCountries(ctx context.Context, req *pb.GetCountriesRequest) (*pb.GetCountriesResponse, error) {
	if req == nil {
		req = &pb.GetCountriesRequest{}
	}
	r, err := s.client.GetCountries(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить список стран")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetCountries())).Msg("список стран получен")
	return r, nil
}

func (s *InstrumentsService) FindInstrument(ctx context.Context, req *pb.FindInstrumentRequest) (*pb.FindInstrumentResponse, error) {
	if req == nil {
		req = &pb.FindInstrumentRequest{}
	}
	r, err := s.client.FindInstrument(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("query", req.GetQuery()).Msg("не удалось выполнить поиск инструментов")
		return nil, err
	}
	s.log.Info().Str("query", req.GetQuery()).Int("count", len(r.GetInstruments())).Msg("поиск инструментов выполнен")
	return r, nil
}

func (s *InstrumentsService) GetBrands(ctx context.Context, req *pb.GetBrandsRequest) (*pb.GetBrandsResponse, error) {
	if req == nil {
		req = &pb.GetBrandsRequest{}
	}
	r, err := s.client.GetBrands(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить бренды")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetBrands())).Msg("бренды получены")
	return r, nil
}

func (s *InstrumentsService) GetBrandBy(ctx context.Context, req *pb.GetBrandRequest) (*pb.Brand, error) {
	if req == nil {
		req = &pb.GetBrandRequest{}
	}
	r, err := s.client.GetBrandBy(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("id", req.GetId()).Msg("не удалось получить бренд")
		return nil, err
	}
	s.log.Info().Str("name", r.GetName()).Msg("бренд получен")
	return r, nil
}

func (s *InstrumentsService) GetAssetFundamentals(ctx context.Context, req *pb.GetAssetFundamentalsRequest) (*pb.GetAssetFundamentalsResponse, error) {
	if req == nil {
		req = &pb.GetAssetFundamentalsRequest{}
	}
	r, err := s.client.GetAssetFundamentals(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Int("assets", len(req.GetAssets())).Msg("не удалось получить фундаментальные показатели")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetFundamentals())).Msg("фундаментальные показатели получены")
	return r, nil
}

func (s *InstrumentsService) GetAssetReports(ctx context.Context, req *pb.GetAssetReportsRequest) (*pb.GetAssetReportsResponse, error) {
	if req == nil {
		req = &pb.GetAssetReportsRequest{}
	}
	r, err := s.client.GetAssetReports(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("instrument_id", req.GetInstrumentId()).Msg("не удалось получить расписания отчетностей")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetEvents())).Msg("расписания отчетностей получены")
	return r, nil
}

func (s *InstrumentsService) GetConsensusForecasts(ctx context.Context, req *pb.GetConsensusForecastsRequest) (*pb.GetConsensusForecastsResponse, error) {
	if req == nil {
		req = &pb.GetConsensusForecastsRequest{}
	}
	r, err := s.client.GetConsensusForecasts(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить консенсус-прогнозы")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetItems())).Msg("консенсус-прогнозы получены")
	return r, nil
}

func (s *InstrumentsService) GetForecastBy(ctx context.Context, req *pb.GetForecastRequest) (*pb.GetForecastResponse, error) {
	if req == nil {
		req = &pb.GetForecastRequest{}
	}
	r, err := s.client.GetForecastBy(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("instrument_id", req.GetInstrumentId()).Msg("не удалось получить прогнозы инвестдомов")
		return nil, err
	}
	s.log.Info().Int("targets", len(r.GetTargets())).Msg("прогнозы инвестдомов получены")
	return r, nil
}

func (s *InstrumentsService) GetRiskRates(ctx context.Context, req *pb.RiskRatesRequest) (*pb.RiskRatesResponse, error) {
	if req == nil {
		req = &pb.RiskRatesRequest{}
	}
	r, err := s.client.GetRiskRates(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить ставки риска")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetInstrumentRiskRates())).Msg("ставки риска получены")
	return r, nil
}

func (s *InstrumentsService) GetInsiderDeals(ctx context.Context, req *pb.GetInsiderDealsRequest) (*pb.GetInsiderDealsResponse, error) {
	if req == nil {
		req = &pb.GetInsiderDealsRequest{}
	}
	r, err := s.client.GetInsiderDeals(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("instrument_id", req.GetInstrumentId()).Msg("не удалось получить сделки инсайдеров")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetInsiderDeals())).Msg("сделки инсайдеров получены")
	return r, nil
}

func (s *InstrumentsService) StructuredNoteBy(ctx context.Context, req *pb.InstrumentRequest) (*pb.StructuredNoteResponse, error) {
	if req == nil {
		req = &pb.InstrumentRequest{}
	}
	r, err := s.client.StructuredNoteBy(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Str("id", req.GetId()).Msg("не удалось получить структурную ноту")
		return nil, err
	}
	s.log.Info().Str("ticker", r.GetInstrument().GetTicker()).Str("figi", r.GetInstrument().GetFigi()).Msg("структурная нота получена")
	return r, nil
}

func (s *InstrumentsService) StructuredNotes(ctx context.Context, req *pb.InstrumentsRequest) (*pb.StructuredNotesResponse, error) {
	if req == nil {
		req = &pb.InstrumentsRequest{}
	}
	r, err := s.client.StructuredNotes(ctx, req)
	if err != nil {
		s.log.Error().Err(err).Msg("не удалось получить список структурных нот")
		return nil, err
	}
	s.log.Info().Int("count", len(r.GetInstruments())).Msg("список структурных нот получен")
	return r, nil
}
