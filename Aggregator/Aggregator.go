package Aggregator

import (
	"database/sql"
	"engine-socket/Clickhouse"
	"engine-socket/Deserializer"
	"engine-socket/Logger"
	"strings"

	"go.uber.org/zap"
)

type Aggregator struct {
	tx      *sql.Tx
	connect *sql.DB
	stmt    *sql.Stmt
}

func NewAggregator() *Aggregator {
	return &Aggregator{}
}

func (a *Aggregator) begin() error {
	var err error

	if a.connect == nil {
		a.connect, err = Clickhouse.Connect()
	}

	if a.connect != nil && a.tx == nil {
		a.tx, err = a.connect.Begin()
	}

	return err
}

func (a *Aggregator) Commit() {
	logger := Logger.GetLogger()

	if a.tx != nil {
		if err := a.tx.Commit(); err != nil {
			logger.Info("Cant't commit query", zap.Error(err))
		}
	}
}

func (a *Aggregator) Close() {
	if a.stmt != nil {
		a.stmt.Close()
	}

	if a.connect != nil {
		a.connect.Close()
	}
}

func (a *Aggregator) AddNetIfaceBatch(interfaceIndex string, dpiInstance string, mapValue Deserializer.Dictionary) (uint16, error) {
	var (
		vlan   uint16      = SHORT_VLAN
		err    error       = nil
		logger *zap.Logger = Logger.GetLogger()
	)

	err = a.begin()

	if err != nil {
		logger.Error("Can't begin transaction ", zap.Error(err))
		return vlan, err
	}

	if a.stmt == nil {
		a.stmt, _ = Clickhouse.PrepareStatement(a.tx, networkInterfaceStatisticStatement)
	}

	timestamp := parseValueToLong(mapValue[TIMESTAMP]) / 1000
	bytes := parseValueToLong(mapValue[BYTES])
	pkts := parseValueToLong(mapValue[PKTS])
	sessions := parseValueToLong(mapValue[SESSIONS])
	outPkts := parseValueToLong(mapValue[OUT_PKTS])
	outBytes := parseValueToLong(mapValue[OUT_BYTES])

	protocol := parseValueToString(mapValue[PROTO])

	vlan = parseValueToVlan(mapValue[VLAN])

	if _, err = a.stmt.Exec(
		interfaceIndex,
		dpiInstance,
		timestamp,
		bytes,
		pkts,
		sessions,
		outPkts,
		outBytes,
		protocol,
		vlan,
	); err != nil {
		logger.Error("Can't execute statement AddNetIfaceBatch", zap.Error(err))
		return vlan, err
	}

	return vlan, err
}

func (a *Aggregator) AddVlanBatch(interfaceIndex string, dpiInstance string, vlan uint16) error {
	var (
		err    error       = a.begin()
		logger *zap.Logger = Logger.GetLogger()
	)

	if err != nil {
		logger.Error("Can't begin transaction", zap.Error(err))
		return err
	}

	if a.stmt == nil {
		a.stmt, _ = Clickhouse.PrepareStatement(a.tx, vlanStatement)
	}

	if _, err = a.stmt.Exec(interfaceIndex, dpiInstance, vlan); err != nil {
		logger.Error("Can't execute statement AddVlanBatch", zap.Error(err))
		return err
	}

	return err
}

func (a *Aggregator) AddDnsBatch(interfaceIndex string, dpiInstance string, mapValue Deserializer.Dictionary) error {
	var (
		err    error       = a.begin()
		logger *zap.Logger = Logger.GetLogger()
	)

	if err != nil {
		logger.Error("Can't begin transaction", zap.Error(err))
		return err
	}

	if a.stmt == nil {
		a.stmt, _ = Clickhouse.PrepareStatement(a.tx, dnsStatement)
	}

	domain, ok := mapValue[DNS_NAME].(string)

	if !ok {
		domain = ""
	} else {
		if strings.HasPrefix(domain, www) {
			domain = domain[len(www):]
		}
	}

	ttl := parseValueToLong(mapValue[TTL])
	count := parseValueToInt(mapValue[COUNT])
	timestamp := parseValueToLong(mapValue[TIMESTAMP])

	ip := ""

	if mapValue[ADDRV6] != nil {
		ip = parseValueToIpv6(mapValue[ADDRV6])
	} else {
		ip = parseValueToIpv4(mapValue[ADDR])
	}

	if _, err = a.stmt.Exec(
		interfaceIndex,
		dpiInstance,
		domain,
		ttl,
		count,
		timestamp,
		ip,
	); err != nil {
		logger.Error("Can't execute statement AddDnsBatch", zap.Error(err))
		return err
	}

	return err
}

func (a *Aggregator) AddNetSessionBatch(interfaceIndex string, dpiInstance string, mapValue Deserializer.Dictionary, caption string) error {
	var (
		err    error       = a.begin()
		logger *zap.Logger = Logger.GetLogger()
	)

	if err != nil {
		logger.Error("Can't begin transaction", zap.Error(err))
		return err
	}

	if a.stmt == nil {
		a.stmt, _ = Clickhouse.PrepareStatement(a.tx, netSessionStatement)
	}

	protocol := parseValueToString(mapValue[PROTOCOL])
	groupId := parseValueToString(mapValue[GROUP_ID])
	serverPort := parseValueToInt(mapValue[SERVER_PORT])
	clientPort := parseValueToInt(mapValue[CLIENT_PORT])

	startTime := parseValueToLong(mapValue[START_TIME]) / 1000
	endTime := parseValueToLong(mapValue[END_TIME]) / 1000

	state := parseValueToLong(mapValue[STATE])

	macServer := parseValueToLong(mapValue[MAC_SERVER])
	macClient := parseValueToLong(mapValue[MAC_CLIENT])

	rtt := parseValueToLong(mapValue[RTT]) / 1000
	art := parseValueToLong(mapValue[ART]) / 1000

	fromSrvPckts := parseValueToLong(mapValue[FROM_SRV_PCKTS])
	fromSrvBytes := parseValueToLong(mapValue[FROM_SRV_BYTES])
	fromSrvPayload := parseValueToLong(mapValue[FROM_SRV_PAYLOAD])

	fromClntPckts := parseValueToLong(mapValue[FROM_CLNT_PKTS])
	fromClntBytes := parseValueToLong(mapValue[FROM_CLNT_BYTES])
	fromClntPayload := parseValueToLong(mapValue[FROM_CLNT_PAYLOAD])

	fragmentsFromSrv := parseValueToLong(mapValue[FRAGMENTS_FROM_SRV])
	fragmentsBytesFromSrv := parseValueToLong(mapValue[FRAGMENTS_BYTES_FROM_SRV])
	fragmentsFromClnt := parseValueToLong(mapValue[FRAGMENTS_FROM_CLNT])
	fragmentsBytesFromClnt := parseValueToLong(mapValue[FRAGMENTS_BYTES_FROM_CLNT])

	reorderFromSrv := parseValueToLong(mapValue[REORDER_FROM_SRV])
	reorderBytesFromSrv := parseValueToLong(mapValue[REORDER_BYTES_FROM_SRV])
	reorderFromClnt := parseValueToLong(mapValue[REORDER_FROM_CLNT])
	reorderBytesFromClnt := parseValueToLong(mapValue[REORDER_BYTES_FROM_CLNT])

	retransPktsFromSrv := parseValueToLong(mapValue[RETRANS_PKTS_FROM_SRV])
	retransBytesFromSrv := parseValueToLong(mapValue[RETRANS_BYTES_FROM_SRV])
	retransPktsFromClnt := parseValueToLong(mapValue[RETRANS_PKTS_FROM_CLNT])
	retransBytesFromClnt := parseValueToLong(mapValue[RETRANS_BYTES_FROM_CLNT])

	lostFromSrv := parseValueToLong(mapValue[LOST_FROM_SRV])
	lostFromClnt := parseValueToLong(mapValue[LOST_FROM_CLNT])

	tos := parseValueToLong(mapValue[TOS])
	cos := parseValueToLong(mapValue[COS])
	cif := parseValueToLong(mapValue[CIF])
	mpls := parseValueToLong(mapValue[MPLS])

	version := parseValueToString(mapValue[VERSION])

	httpUrl := ""
	httpMethod := ""
	httpResponse := ""
	httpContext := ""
	httpForwarded := ""
	httpOrigin := ""
	httpCookie := ""
	httpXSessionType := ""
	httpUserAgent := ""
	httpEncoding := ""

	if caption == HTTP {
		httpUrl = parseValueToString(mapValue[HTTP_URL])
		httpMethod = parseValueToString(mapValue[HTTP_METHOD])
		httpResponse = parseValueToString(mapValue[HTTP_RESPONSE])
		httpContext = parseValueToString(mapValue[HTTP_CONTEXT])
		httpForwarded = parseValueToString(mapValue[HTTP_FORWARDED])
		httpOrigin = parseValueToString(mapValue[HTTP_ORIGIN])
		httpCookie = parseValueToString(mapValue[HTTP_COOKIE])
		httpXSessionType = parseValueToString(mapValue[HTTP_X_SESSION_TYPE])
		httpUserAgent = parseValueToString(mapValue[HTTP_USER_AGENT])
		httpEncoding = parseValueToString(mapValue[HTTP_ENCODING])
	}

	clientIP := ""
	serverIP := ""

	if mapValue[SERVER_IP6] != nil {
		serverIP = parseValueToIpv6(mapValue[SERVER_IP6])
	} else {
		serverIP = parseValueToIpv4(mapValue[SERVER_IP])
	}

	if mapValue[CLIENT_IP6] != nil {
		clientIP = parseValueToIpv6(mapValue[CLIENT_IP6])
	} else {
		clientIP = parseValueToIpv4(mapValue[CLIENT_IP6])
	}

	isBroadcastClient := 0
	isBroadcastServer := 0

	if strings.HasSuffix(clientIP, BROADCAST) || clientIP == DEFAULT_IP6_ROUTE {
		isBroadcastClient = 1
	}

	if strings.HasSuffix(serverIP, BROADCAST) || serverIP == DEFAULT_IP6_ROUTE {
		isBroadcastServer = 1
	}

	domain := ""
	serverCertificate := ""
	clientCertificate := ""

	if caption == SSL {
		serverCertificate = parseValueToString(mapValue[SERVER_CERTIFICATE])

		clientCertificate = parseValueToString(mapValue[CLIENT_CERTIFICATE])

		if len(serverCertificate) != 0 {
			if strings.HasSuffix(serverCertificate, CERTIFICATE_DOMAIN_PREFIX) {
				domain = serverCertificate[2:]
			}
		} else if len(clientCertificate) != 0 {
			if strings.HasSuffix(clientCertificate, CERTIFICATE_DOMAIN_PREFIX) {
				domain = clientCertificate[2:]
			}
		}
	}

	vlan := parseValueToVlan(mapValue[VLAN])

	if _, err = a.stmt.Exec(
		interfaceIndex,
		dpiInstance,
		protocol,
		groupId,
		serverPort,
		clientPort,
		startTime,
		endTime,
		domain,
		isBroadcastServer,
		isBroadcastClient,
		state,
		macServer,
		macClient,
		groupId,
		rtt,
		art,
		fromSrvPckts,
		fromSrvBytes,
		fromSrvPayload,
		fromClntPckts,
		fromClntBytes,
		fromClntPayload,
		fragmentsFromSrv,
		fragmentsBytesFromSrv,
		fragmentsFromClnt,
		fragmentsBytesFromClnt,
		reorderFromSrv,
		reorderBytesFromSrv,
		reorderFromClnt,
		reorderBytesFromClnt,
		retransPktsFromSrv,
		retransBytesFromSrv,
		retransPktsFromClnt,
		retransBytesFromClnt,
		lostFromSrv,
		lostFromClnt,
		tos,
		cos,
		cif,
		mpls,
		clientCertificate,
		serverCertificate,
		version,
		httpUrl,
		httpMethod,
		httpResponse,
		httpContext,
		httpForwarded,
		httpOrigin,
		httpCookie,
		httpXSessionType,
		httpUserAgent,
		httpEncoding,
		serverIP,
		clientIP,
		vlan,
		vlan,
		caption,
	); err != nil {
		logger.Error("Can't execute statement AddNetSessionBatch", zap.Error(err))
		return err
	}

	return err
}

func (a *Aggregator) Execute() {
	a.Commit()

	a.Close()
}
