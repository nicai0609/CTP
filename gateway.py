import thosttraderapi as tdapi
import thostmduserapi as mdapi
import pandas as pd
from datetime import datetime
import time
import uuid


MDID = "tcp://180.168.146.187:10011"
TDID = "tcp://180.168.146.187:10101"
BROKERID = "9999"
USERID = ""
PASSWORD = ""
APPID = "simnow_client_test"
AUTHCODE = "0000000000000000"

class GatewayOrder(object):
	__slots__ = ('code','amount','side','action','price','order_status','order_id')
	def __init__(self, code, amount, side, action, price):
		self.code = code
		self.amount = amount
		self.side = side
		self.action = action
		self.price = price
		self.order_status = 0 # 0未成交 1成交
		self.order_id = uuid.uuid4().hex

class TdSpi(tdapi.CThostFtdcTraderSpi):
	# 继承重写c++的tdapi方法，交易连接
	def __init__(self):
		tdapi.CThostFtdcTraderSpi.__init__(self)
		self.api = tdapi.CThostFtdcTraderApi_CreateFtdcTraderApi()
		self.login = False
		self.lock = False # 锁机制，用于确保系统前后操作，下单超过1秒废单
		self.position = None
		self.positionCache = {}
		self.account = {}
		self.contract = {}
		self.orderList = []

	def OnFrontConnected(self):
		obj = tdapi.CThostFtdcReqAuthenticateField()
		obj.BrokerID = BROKERID
		obj.UserID = USERID
		obj.AppID = APPID
		obj.AuthCode = AUTHCODE
		self.api.ReqAuthenticate(obj, 0)

	def OnRspAuthenticate(self, pRspAuthenticateField: 'CThostFtdcRspAuthenticateField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool'):
		if not pRspInfo.ErrorID :
			obj = tdapi.CThostFtdcReqUserLoginField()
			obj.BrokerID = BROKERID
			obj.UserID = USERID
			obj.Password = PASSWORD
			obj.UserProductInfo = "python dll"
			self.api.ReqUserLogin(obj, 0)
		else:
			print('认证错误 {}'.format(pRspInfo.ErrorMsg))

	def OnFrontDisconnected(self, nReason: 'int'):
		# 服务器断开
		print('交易服务器断开 !')
		self.login = False

	def OnRspUserLogin(self, pRspUserLogin: 'CThostFtdcRspUserLoginField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool'):
		# 账户登录
		if not pRspInfo.ErrorID:
			print('用户登录')
			obj = tdapi.CThostFtdcQrySettlementInfoField()
			obj.BrokerID = BROKERID
			obj.InvestorID = USERID
			obj.TradingDay = pRspUserLogin.TradingDay
			self.api.ReqQrySettlementInfo(obj, 0)
		else:
			print('登录错误 {}'.format(pRspInfo.ErrorMsg))

	def OnRspQrySettlementInfo(self, pSettlementInfo: 'CThostFtdcSettlementInfoField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool'):
		# 结算确认
		if pSettlementInfo is not None :
			print('content {}'.format(pSettlementInfo.Content))
		if bIsLast :
			obj = tdapi.CThostFtdcSettlementInfoConfirmField()
			obj.BrokerID = BROKERID
			obj.InvestorID = USERID
			self.api.ReqSettlementInfoConfirm(obj, 0)
			print('结算确认')

	def OnRspSettlementInfoConfirm(self, pSettlementInfoConfirm: 'CThostFtdcSettlementInfoConfirmField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool'):
		# 获取交易结算信息
		if not pRspInfo.ErrorID:
			self.login = True
		else:
			print('结算错误 {}'.format(pRspInfo.ErrorMsg))

	def OnRtnOrder(self, pOrder: 'CThostFtdcOrderField'):
		# 需要根据报单确定是否成交，未成交需要通知交易系统进行处理，需要维护报单列表
		print('OrderStatus={}, StatusMsg={}, LimitPrice={}'.format(pOrder.OrderStatus, pOrder.StatusMsg, pOrder.LimitPrice))
	
	def OnRtnTrade(self, pTrade: 'CThostFtdcTradeField'):
		# 由于交易所返回持仓有延迟，所以需要自己维护持仓列表或者在交易系统根据成交回报维护持仓列表
		# ExchangeID 交易所代码
		# TradeID 成交编号
		# Direction 买卖方向
		# OrderRef 报单引用
		# OrderSysID 报单编号
		# TraderID 交易所交易员代码
		# OrderLocalID 本地报单编号
		# InstrumentID 合约代码
		# Direction 买卖方向
		# OffsetFlag 开平标志
		# Price 价格
		# Volume 数量
		exchangeID = pTrade.ExchangeID
		tradeID = pTrade.TradeID
		side = 'long' if pTrade.Direction==0 else 'short' # 0 多 1 空
		orderRef = pTrade.OrderRef
		orderSysID = pTrade.OrderSysID
		traderID = pTrade.TraderID
		orderLocalID = pTrade.OrderLocalID
		code = pTrade.InstrumentID
		offsetFlag = 'buy' if pTrade.OffsetFlag==0 else 'sell' # 0 买 1 卖
		price = pTrade.Price
		amount = pTrade.Volume

		index = (self.position.index==code)&(self.position.side==side)
		if offsetFlag == 'buy':			
			self.position.loc[index, 'amount'] += amount
			self.position.loc[index, 'cost'] += amount * price
		elif offsetFlag == 'sell':
			self.position.loc[index, 'amount'] += amount
			self.position.loc[index, 'cost'] += amount * price
		self.position.loc[index, 'avg_cost'] = self.position['cost']/(self.position['size']*self.position['amount'])
		self.position['yd_amount'] = self.position['amount']-self.position['td_amount']
		print(exchangeID, offsetFlag, side, amount, code, price)
		print(self.position)

	def OnRspOrderInsert(self, pInputOrder: 'CThostFtdcInputOrderField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool'):
		# 订单反馈
		if pRspInfo.ErrorID:
			print('订单错误 {} {}'.format(pRspInfo.ErrorID, pRspInfo.ErrorMsg))	

	def OnRspQryInvestorPosition(self, pInvestorPosition: 'CThostFtdcInvestorPositionField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool'):
		# InstrumentID 合约代码
		# BrokerID 经纪公司代码
		# InvestorID 投资者代码
		# PosiDirection 持仓多空方向，'2'表示多头持仓，'3'表示空头持仓
		# HedgeFlag 投机套保标志
		# PositionDate 持仓日期，区分是否历史仓的枚举值，1表示当前交易日持仓，2表示是历史仓（昨仓）
		# YdPosition 上日持仓
		# Position 总持仓
		# TodayPosition 今日持仓
		# LongFrozen 多头冻结
		# ShortFrozen 空头冻结
		# OpenVolume 开仓量，当天该键值上总的开仓量  
		# CloseVolume 平仓量, 当天该键值上总的平仓量   
		# PositionCost 持仓成本, 当天新开仓按开仓价计算，昨仓则是用昨结算价计算，计算公式为price*volume*RateMultiple    
		# OpenCost 开仓成本, 新老仓都是按照开仓价计算的成本，如果无昨仓与持仓成本字段是相同的值
		# CloseProfit 平仓盈亏, 等于下面的逐日盯市平仓盈亏 
		# PositionProfit 持仓盈亏, 按最新价计算出来的持仓值与持仓成本的差值
		# CloseProfitByDate 逐日盯市平仓盈亏,  昨仓是平仓价与昨结算价计算出的盈亏，今仓是平仓价与开仓价计算出的盈亏 ，计算公式为（closeprice - openprice或preSettlementPrice）*volume*RateMultiple 
		# CloseProfitByTrade 逐笔对冲平仓盈亏, 平仓价与开仓价计算出的盈亏 
		# MarginRateByMoney 保证金率,  该合约的交易保证金率，同查询所得值一致。昨仓无此值
		# MarginRateByVolume 保证金率(按手数), 该合约的交易保证金率(按手数)，同查询所得值一致。昨仓无此值
		code = pInvestorPosition.InstrumentID
		amount = pInvestorPosition.Position
		td_amount = pInvestorPosition.TodayPosition
		side = 'long' if pInvestorPosition.PosiDirection=='2' else 'short'
		cost = pInvestorPosition.PositionCost
		profit = pInvestorPosition.PositionProfit
		print(code, amount, td_amount, side)
		key = code+side
		# 上期所持仓的今昨分条返回(有昨仓、无今仓)
		if amount>0:
			if key in self.positionCache:
				self.positionCache[key]['amount'] += amount
				self.positionCache[key]['td_amount'] += td_amount
				self.positionCache[key]['cost'] += cost
				self.positionCache[key]['profit'] += profit
			# 其它交易所统一返回
			else:
				self.positionCache[key] = {'code':code,'amount':amount,'td_amount':td_amount,'side':side,'cost':cost,'profit':profit}
		
		if bIsLast:
			self.position = pd.DataFrame(self.positionCache.values())
			self.position = self.position.set_index('code')
			self.position['size'] = self.contract.loc[self.position.index, 'size']
			self.position['avg_cost'] = self.position['cost']/(self.position['size']*self.position['amount'])
			self.position['yd_amount'] = self.position['amount']-self.position['td_amount']
			print(self.position)
			self.positionCache.clear()

	def OnRspQryTradingAccount(self, pTradingAccount: 'CThostFtdcTradingAccountField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool'):
		# 账户查询反馈
		self.account['balance'] = pTradingAccount.Balance
		self.account['available'] = pTradingAccount.Available
		self.account['commission'] = pTradingAccount.Commission
		self.account['margin'] = pTradingAccount.CurrMargin
		self.account['closeProfit'] = pTradingAccount.CloseProfit
		self.account['positionProfit'] = pTradingAccount.PositionProfit
		print(self.account)

	def OnRspQryInstrument(self, pInstrumentMarginRate: 'CThostFtdcInstrumentMarginRateField', pRspInfo: 'CThostFtdcRspInfoField', nRequestID: 'int', bIsLast: 'bool'):
		# 合约查询反馈
		code = pInstrumentMarginRate.InstrumentID
		exchange = pInstrumentMarginRate.ExchangeID
		name = pInstrumentMarginRate.InstrumentName
		size = pInstrumentMarginRate.VolumeMultiple
		priceTick = pInstrumentMarginRate.PriceTick
		strikePrice = pInstrumentMarginRate.StrikePrice
		productClass = pInstrumentMarginRate.ProductClass
		expiryDate = pInstrumentMarginRate.ExpireDate
		optionsType = pInstrumentMarginRate.OptionsType
		self.contract[code] = {'code':code, 'exchange':exchange, 'name':name, 
							   'size':size, 'priceTick':priceTick, 'strikePrice':strikePrice,
							   'productClass':productClass, 'expiryDate':expiryDate, 'optionsType':optionsType}
		if bIsLast:
			self.contract = pd.DataFrame(self.contract.values())
			self.contract = self.contract.set_index('code')
			print(self.contract)
			self.lock = False

	def connect(self):
		# 连接
		self.api.RegisterFront(TDID)
		self.api.RegisterSpi(self)
		self.api.SubscribePrivateTopic(tdapi.THOST_TERT_QUICK)
		self.api.SubscribePublicTopic(tdapi.THOST_TERT_QUICK)				
		self.api.Init()

	def order(self, code, side, amount, price):
		# 下单，简单化处理，1秒内CTP没有成交，视为废单
		assert side in ['long', 'short'], 'side should be long or short !'
		while self.lock==True or self.position is None:
			time.sleep(0.1)
		order = tdapi.CThostFtdcInputOrderField()
		order.BrokerID = BROKERID
		order.ExchangeID = self.contract.loc[code,'exchange'] # 交易所代码
		order.InstrumentID = code # 合约代码
		order.UserID = USERID
		order.InvestorID = USERID
		order.Direction = tdapi.THOST_FTDC_D_Buy if side=='long' else tdapi.THOST_FTDC_D_Sell # 多空
		order.LimitPrice = price # 下单价格
		order.VolumeTotalOriginal = abs(amount) # 下单数量
		order.OrderPriceType = tdapi.THOST_FTDC_OPT_LimitPrice # 市价单、限价单
		order.ContingentCondition = tdapi.THOST_FTDC_CC_Immediately # 立即成交，否则作废
		order.TimeCondition = tdapi.THOST_FTDC_TC_GFD
		order.VolumeCondition = tdapi.THOST_FTDC_VC_AV
		order.CombHedgeFlag = "1" # 投机套保标志
		order.CombOffsetFlag = '0' if amount>0 else '1' # '0'开仓,'1'平仓,'3'平今,'4'平昨
		order.GTDDate = ""
		order.orderfieldRef = "1"
		order.MinVolume = 0
		order.ForceCloseReason = tdapi.THOST_FTDC_FCC_NotForceClose
		order.IsAutoSuspend = 0
 
		# 上期所需要区分平今仓和平昨仓
		if self.contract.loc[code,'exchange']=='SHFE' and amount<0 and abs(amount)>self.position.loc[code,'yd_amount']:
			new_amount = int(self.position.loc[code,'yd_amount'])
			order.VolumeTotalOriginal = new_amount
			order.CombOffsetFlag = '4'
			self.api.ReqOrderInsert(order, 0)
			_order = GatewayOrder(code=code, amount=new_amount, side='short', action='close', price=price)
			self.orderList.append(_order)

			new_amount = int(abs(amount)-self.position.loc[code,'yd_amount'])
			order.VolumeTotalOriginal = new_amount
			order.CombOffsetFlag = '3'
			self.api.ReqOrderInsert(order, 0)
			_order = GatewayOrder(code=code, amount=new_amount, side='short', action='close', price=price)
			self.orderList.append(_order)
		else:
			self.api.ReqOrderInsert(order, 0)
			_order = GatewayOrder(code=code, amount=amount, side=side, action='open' if amount>0 else 'close', price=price)
			self.orderList.append(_order)

	def qryInstrument(self):
		# 查询合约
		if len(self.contract)==0:
			obj = tdapi.CThostFtdcQryInstrumentField()
			self.api.ReqQryInstrument(obj, 0)
			self.lock = True

	def qryPosition(self):
		# 查询持仓
		while self.lock==True:
			time.sleep(0.1)
		obj = tdapi.CThostFtdcQryInvestorPositionField()
		self.api.ReqQryInvestorPosition(obj, 0)

	def qryAccount(self):
		# 查询账户
		while self.lock==True:
			time.sleep(0.1)
		obj = tdapi.CThostFtdcQryTradingAccountField()
		self.api.ReqQryTradingAccount(obj, 0)

	def test(self):
		self.connect()
		while True:
			if self.login:
				self.qryInstrument()
				time.sleep(1)
				if datetime.now().second%10==0:
					self.order(code='rb2101', side='short', amount=-140, price=3900)
					time.sleep(1)
				self.qryPosition()
				time.sleep(1)
				self.qryAccount()
				time.sleep(1)

if __name__ == '__main__':
	t = TdSpi()
	t.test()


