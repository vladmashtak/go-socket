package Aggregator

import (
	"database/sql"
	"engine-socket/Clickhouse"
	"log"
	"strings"
)

type Aggregator struct {
	tx             *sql.Tx
	connect        *sql.DB
	netIfaceStmt   *sql.Stmt
	vlanStmt       *sql.Stmt
	netSessionStmt *sql.Stmt
	dnsStmt        *sql.Stmt
}

func NewAggregator() *Aggregator {
	return &Aggregator{}
}

func (a *Aggregator) begin() error {
	var err error

	if a.connect == nil {
		a.connect, err = Clickhouse.Connect()
	}

	if a.tx == nil {
		a.tx, err = a.connect.Begin()
	}

	return err
}

func (a *Aggregator) commit() {
	if a.tx != nil {
		if err := a.tx.Commit(); err != nil {
			log.Println("Cant't commit query ", err)
		}
	}
}

func (a *Aggregator) close() {
	if a.connect != nil {
		a.connect.Close()
	}
}

func (a *Aggregator) AddNetIfaceBatch(interfaceIndex string, dpiInstance string, mapValue map[string]interface{}) {

	if err := a.begin(); err != nil {
		log.Println("Can't begin transaction ", err)
		return
	}

	if a.netIfaceStmt == nil {
		a.netIfaceStmt, _ = Clickhouse.PrepareStatement(a.tx, networkInterfaceStatisticStatement)
	}

	timestamp := parseValueToLong(mapValue["timestamp"]) / 1000
	bytes := parseValueToLong(mapValue["bytes"])
	pkts := parseValueToLong(mapValue["pkts"])
	session := parseValueToLong(mapValue["session"])
	outPkts := parseValueToLong(mapValue["out_pkts"])
	outBytes := parseValueToLong(mapValue["out_bytes"])

	protocol := mapValue["proto"].(string)

	vlan := parseValueToVlan(mapValue["vlan"])

	if _, err := a.netIfaceStmt.Exec(
		interfaceIndex,
		dpiInstance,
		timestamp,
		bytes,
		pkts,
		session,
		outPkts,
		outBytes,
		protocol,
		vlan,
	); err != nil {
		log.Println("Can't execute statement AddNetIfaceBatch ", err)
	}

	// a.AddVlanBatch(interfaceIndex, dpiInstance, vlan)
}

func (a *Aggregator) AddVlanBatch(interfaceIndex string, dpiInstance string, vlan uint16) {

	if err := a.begin(); err != nil {
		log.Println("Can't begin transaction ", err)
		return
	}

	if a.vlanStmt == nil {
		a.vlanStmt, _ = Clickhouse.PrepareStatement(a.tx, vlanStatement)
	}

	if _, err := a.vlanStmt.Exec(interfaceIndex, dpiInstance, vlan); err != nil {
		log.Println("Can't execute statement AddVlanBatch ", err)
	}
}

func (a *Aggregator) AddDnsBatch(interfaceIndex string, dpiInstance string, mapValue map[string]interface{}) {
	if err := a.begin(); err != nil {
		log.Println("Can't begin transaction ", err)
		return
	}

	if a.dnsStmt == nil {
		a.dnsStmt, _ = Clickhouse.PrepareStatement(a.tx, dnsStatement)
	}

	domain, ok := mapValue["c_name"].(string)

	if !ok {
		domain = ""
	} else {
		if strings.HasPrefix(domain, "www") {
			domain = domain[len("www"):]
		}
	}

	ttl := parseValueToLong(mapValue["ttl"])
	count := parseValueToInt(mapValue["count"])
	timestamp := parseValueToLong(mapValue["timestamp"])

	ip := ""

	if mapValue["addrv6"] != nil {
		ip = parseValueToIpv6(mapValue["addrv6"])
	} else {
		ip = parseValueToIpv4(mapValue["addr"])
	}

	if _, err := a.dnsStmt.Exec(
		interfaceIndex,
		dpiInstance,
		domain,
		ttl,
		count,
		timestamp,
		ip,
	); err != nil {
		log.Println("Can't execute statement AddDnsBatch ", err)
	}
}

func (a *Aggregator) AddNetSessionBatch(interfaceIndex string, dpiInstance string, mapValue map[string]interface{}, caption string) {

	if err := a.begin(); err != nil {
		log.Println("Can't begin transaction ", err)
		return
	}

	if a.netSessionStmt == nil {
		a.netSessionStmt, _ = Clickhouse.PrepareStatement(a.tx, netSessionStatement)
	}

	protocol := parseValueToString(mapValue["srv_protocol"])
	groupId := parseValueToString(mapValue["group_id"])
	serverPort := parseValueToInt(mapValue["service_port"])
	clientPort := parseValueToInt(mapValue["clnt_port"])

	startTime := parseValueToLong(mapValue["start"]) / 1000
	endTime := parseValueToLong(mapValue["end"]) / 1000

	state := parseValueToLong(mapValue["state"])

	macServer := parseValueToLong(mapValue["mac_srv"])
	macClient := parseValueToLong(mapValue["mac_clnt"])

	rtt := parseValueToLong(mapValue["rtt"]) / 1000
	art := parseValueToLong(mapValue["art"]) / 1000

	fromSrvPckts := parseValueToLong(mapValue["from_srv_pckts"])
	fromSrvBytes := parseValueToLong(mapValue["from_srv_bytes"])
	fromSrvPayload := parseValueToLong(mapValue["from_srv_payload"])

	fromClntPckts := parseValueToLong(mapValue["from_clntpckts"])
	fromClntBytes := parseValueToLong(mapValue["from_clnt_bytes"])
	fromClntPayload := parseValueToLong(mapValue["from_clnt_payload"])

	fragmentsFromSrv := parseValueToLong(mapValue["fragments_from_srv"])
	fragmentsBytesFromSrv := parseValueToLong(mapValue["fragments_bytes_from_srv"])
	fragmentsFromClnt := parseValueToLong(mapValue["fragments_from_clnt"])
	fragmentsBytesFromClnt := parseValueToLong(mapValue["fragments_bytes_from_clnt"])

	reorderFromSrv := parseValueToLong(mapValue["reorder_from_srv"])
	reorderBytesFromSrv := parseValueToLong(mapValue["reorder_bytes_from_srv"])
	reorderFromClnt := parseValueToLong(mapValue["reorder_from_clnt"])
	reorderBytesFromClnt := parseValueToLong(mapValue["reorder_bytes_from_clnt"])

	retransPktsFromSrv := parseValueToLong(mapValue["retrans_pkts_from_srv"])
	retransBytesFromSrv := parseValueToLong(mapValue["retrans_bytes_from_srv"])
	retransPktsFromClnt := parseValueToLong(mapValue["retrans_pkts_from_clnt"])
	retransBytesFromClnt := parseValueToLong(mapValue["retrans_bytes_from_clnt"])

	lostFromSrv := parseValueToLong(mapValue["lost_from_srv"])
	lostFromClnt := parseValueToLong(mapValue["lost_from_clnt"])

	tos := parseValueToLong(mapValue["tos"])
	cos := parseValueToLong(mapValue["cos"])
	cif := parseValueToLong(mapValue["cif"])
	mpls := parseValueToLong(mapValue["mpls"])

	version := parseValueToString(mapValue["version"])

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

	if strings.ToUpper(caption) == "HTTP" {
		httpUrl = parseValueToString(mapValue["http_url"])
		httpMethod = parseValueToString(mapValue["http_method"])
		httpResponse = parseValueToString(mapValue["http_response"])
		httpContext = parseValueToString(mapValue["http_context"])
		httpForwarded = parseValueToString(mapValue["http_forwarded"])
		httpOrigin = parseValueToString(mapValue["http_origin"])
		httpCookie = parseValueToString(mapValue["http_cookie"])
		httpXSessionType = parseValueToString(mapValue["http_x_session_type"])
		httpUserAgent = parseValueToString(mapValue["http_user_agent"])
		httpEncoding = parseValueToString(mapValue["http_encoding"])
	}

	clientIP := ""
	serverIP := ""

	if mapValue["service_ipv6"] != nil {
		serverIP = parseValueToIpv6(mapValue["service_ipv6"])
	} else {
		serverIP = parseValueToIpv4(mapValue["service_ip"])
	}

	if mapValue["clnt_ipv6"] != nil {
		clientIP = parseValueToIpv6(mapValue["clnt_ipv6"])
	} else {
		clientIP = parseValueToIpv4(mapValue["clnt_ip"])
	}

	isBroadcastClient := 0
	isBroadcastServer := 0

	if strings.HasSuffix(clientIP, "255") || clientIP == "0:0:0:0:0:0:0:0" {
		isBroadcastClient = 1
	}

	if strings.HasSuffix(serverIP, "255") || serverIP == "0:0:0:0:0:0:0:0" {
		isBroadcastServer = 1
	}

	domain := ""
	serverCertificate := ""
	clientCertificate := ""

	if strings.ToUpper(caption) == "SSL" {
		serverCertificate = parseValueToString(mapValue["server_certificate"])

		clientCertificate = parseValueToString(mapValue["client_certificate"])

		if len(serverCertificate) != 0 {
			if strings.HasSuffix(serverCertificate, "*.") {
				domain = serverCertificate[2:]
			}
		} else if len(clientCertificate) != 0 {
			if strings.HasSuffix(clientCertificate, "*.") {
				domain = clientCertificate[2:]
			}
		}
	}

	vlan := parseValueToVlan(mapValue["vlan"])

	if _, err := a.netSessionStmt.Exec(
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
		log.Println("Can't execute statement AddNetSessionBatch ", err)
	}
}

func (a *Aggregator) Execute() {

	a.commit()

	if a.netIfaceStmt != nil {
		a.netIfaceStmt.Close()
	}

	if a.vlanStmt != nil {
		a.vlanStmt.Close()
	}

	if a.dnsStmt != nil {
		a.dnsStmt.Close()
	}

	if a.netSessionStmt != nil {
		a.netSessionStmt.Close()
	}

	a.connect.Close()
}
