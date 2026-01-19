# DhanHQ API Annexure & Error Codes

## Exchange Segment Mapping
| Attribute | Exchange | Segment | Value |
|-----------|----------|---------|-------|
| `IDX_I` | Index | Index Value | 0 |
| `NSE_EQ` | NSE | Equity Cash | 1 |
| `NSE_FNO` | NSE | Futures & Options | 2 |
| `NSE_CURRENCY` | NSE | Currency | 3 |
| `BSE_EQ` | BSE | Equity Cash | 4 |
| `MCX_COMM` | MCX | Commodity | 5 |
| `BSE_CURRENCY` | BSE | Currency | 7 |
| `BSE_FNO` | BSE | Futures & Options | 8 |

## Product Type (Intraday Only: CO & BO)
| Attribute | Detail |
|-----------|--------|
| `CNC` | Cash & Carry for equity deliveries |
| `INTRADAY` | Intraday for Equity, Futures & Options |
| `MARGIN` | Carry Forward in Futures & Options |
| `CO` | Cover Order |
| `BO` | Bracket Order |

## Order Status
| Attribute | Detail |
|-----------|--------|
| `TRANSIT` | Did not reach the exchange server |
| `PENDING` | Awaiting execution |
| `CLOSED` | Used for Super Order, once both entry and exit orders are placed |
| `TRIGGERED` | Used for Super Order, if Target or Stop Loss leg is triggered |
| `REJECTED` | Rejected by broker/exchange |
| `CANCELLED` | Cancelled by user |
| `PART_TRADED` | Partial Quantity traded successfully |
| `TRADED` | Executed successfully |

## After Market Order (AMO) Time
| Attribute | Detail |
|-----------|--------|
| `PRE_OPEN` | Pumped at pre-market session |
| `OPEN` | Pumped at market open |
| `OPEN_30` | Pumped 30 minutes after market open |
| `OPEN_60` | Pumped 60 minutes after market open |

## Expiry Code
| Attribute | Detail |
|-----------|--------|
| `0` | Current Expiry/Near Expiry |
| `1` | Next Expiry |
| `2` | Far Expiry |

## Instrument Types
| Attribute | Detail |
|-----------|--------|
| `INDEX` | Index |
| `FUTIDX` | Futures of Index |
| `OPTIDX` | Options of Index |
| `EQUITY` | Equity |
| `FUTSTK` | Futures of Stock |
| `OPTSTK` | Options of Stock |
| `FUTCOM` | Futures of Commodity |
| `OPTFUT` | Options of Commodity Futures |
| `FUTCUR` | Futures of Currency |
| `OPTCUR` | Options of Currency |

## Feed Request Codes
| Code | Detail |
|------|--------|
| `11` | Connect Feed |
| `12` | Disconnect Feed |
| `15` | Subscribe - Ticker Packet |
| `16` | Unsubscribe - Ticker Packet |
| `17` | Subscribe - Quote Packet |
| `18` | Unsubscribe - Quote Packet |
| `21` | Subscribe - Full Packet |
| `22` | Unsubscribe - Full Packet |
| `23` | Subscribe - Full Market Depth |
| `24` | Unsubscribe - Full Market Depth |

## Feed Response Codes
| Code | Detail |
|------|--------|
| `1` | Index Packet |
| `2` | Ticker Packet |
| `4` | Quote Packet |
| `5` | OI Packet |
| `6` | Prev Close Packet |
| `7` | Market Status Packet |
| `8` | Full Packet |
| `50` | Feed Disconnect |

## Trading API Errors
| Type | Code | Message |
|------|------|---------|
| Invalid Authentication | `DH-901` | Client ID or user generated access token is invalid or expired. |
| Invalid Access | `DH-902` | User has not subscribed to Data APIs or does not have access to Trading APIs. |
| User Account | `DH-903` | Errors related to User's Account. Check required segments. |
| Rate Limit | `DH-904` | Too many requests. Try throttling. |
| Input Exception | `DH-905` | Missing required fields, bad values. |
| Order Error | `DH-906` | Incorrect request for order. |
| Data Error | `DH-907` | Unable to fetch data (incorrect params or no data). |
| Internal Server Error | `DH-908` | Server unable to process request. |
| Network Error | `DH-909` | API unable to communicate with backend. |
| Others | `DH-910` | Other reasons. |

## Data API Errors
| Code | Description |
|------|-------------|
| `800` | Internal Server Error |
| `804` | Requested number of instruments exceeds limit |
| `805` | Too many requests. User may be blocked. |
| `806` | Data APIs not subscribed |
| `807` | Access token is expired |
| `808` | Authentication Failed - Client ID or Access Token invalid |
| `809` | Access token is invalid |
| `810` | Client ID is invalid |
| `811` | Invalid Expiry Date |
| `812` | Invalid Date Format |
| `813` | Invalid SecurityId |
| `814` | Invalid Request |
