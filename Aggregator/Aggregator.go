package Aggregator

import (
	"database/sql"
	"engine-socket/Clickhouse"
	"log"
)
const (
	networkInterfaceStatisticStatement =
		`INSERT INTO networkInterfaceStatistic (
			interfaceIndex,
			dpiInstance,
			timestamp,
			totalBytes, 
			totalPackets,
			totalSessions,
			totalOutBytes,
			totalOutPackets,
			protocol,
			vlan
		) VALUES (?,?,?,?,?,?,?,?,?,?)`
	netSessionStatement =
		`INSERT INTO netSession (
			interfaceIndex,	dpiInstance,
			protocol,
			groupId,
			serverPort,	clientPort,
			startTime,endTime,
			domain,
			isBroadcastServer, isBroadcastClient,
			state,
			macServer, macClient,
			simpleObjectId,
			rtt, art,
			from_srv_pkts, from_srv_bytes,
			from_srv_payload, from_clnt_pkts,
			from_clnt_bytes, from_clnt_payload,
			fragments_from_srv, fragments_bytes_from_srv,
			fragments_from_clnt, fragments_bytes_from_clnt,
			reorder_from_srv, reorder_bytes_from_srv,
			reorder_from_clnt, reorder_bytes_from_clnt,
			retrans_pkts_from_srv, retrans_bytes_from_srv,
			retrans_pkts_from_clnt, retrans_bytes_from_clnt,
			lost_from_srv, lost_from_clnt,
			tos, cos, cif, mpls,
			client_certificate, server_certificate,
			version,
			http_url, http_method,
			http_response, http_context,
			http_forwarded, http_origin,
			http_cookie, http_x_session_type,
			http_user_agent, http_encoding,
			serverIP, clientIP,
			vlan1, vlan2,
			source
		) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)`
	dnsStatement =
		`INSERT INTO dns (
			interfaceIndex,
			dpiInstance,
			name,
			ttl,
			count,
			timestamp,
			host
		) VALUES (?,?,?,?,?,?,?)`
	vlanStatement =
		`INSERT INTO vlan (
			interfaceIndex,
			dpiInstance,
			vlan
		) VALUES (?,?,?)`
)

type Aggregator struct {
	tx *sql.Tx
	connect *sql.DB
	netIfaceStmt *sql.Stmt
	vlanStmt *sql.Stmt
	netSessionStmt *sql.Stmt
	dnsStmt *sql.Stmt
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

func (a *Aggregator) AddNetIfaceBatch(interfaceIndex string, dpiInstance string, mapValue map[string]interface{})  {
	if err := a.begin(); err != nil {
		return
	}

	if a.netIfaceStmt == nil {
		a.netIfaceStmt, _ = a.tx.Prepare(networkInterfaceStatisticStatement)
	}

	if _, err := a.netIfaceStmt.Exec(
		interfaceIndex,
		dpiInstance,
	); err != nil {
		log.Fatal(err)
	}
}

func (a *Aggregator) AddVlanBatch(interfaceIndex string, dpiInstance string, vlan uint8) {
	if err := a.begin(); err != nil {
		return
	}

	if a.vlanStmt == nil {
		a.vlanStmt, _ = a.tx.Prepare(vlanStatement)
	}

	if _, err := a.vlanStmt.Exec(interfaceIndex, dpiInstance, vlan); err != nil {
		log.Fatal(err)
	}
}

func (a *Aggregator) AddDnsBatch(interfaceIndex string, dpiInstance string, mapValue map[string]interface{}) {
	if err := a.begin(); err != nil {
		return
	}

	if a.dnsStmt == nil {
		a.dnsStmt, _ = a.tx.Prepare(dnsStatement)
	}

	if _, err := a.dnsStmt.Exec(
	); err != nil {
		log.Fatal(err)
	}
}

func (a *Aggregator) AddNetSessionBatch(interfaceIndex string, dpiInstance string, mapValue map[string]interface{}) {
	if err := a.begin(); err != nil {
		return
	}

	if a.netSessionStmt == nil {
		a.netSessionStmt, _ = a.tx.Prepare(netSessionStatement)
	}

	if _, err := a.netSessionStmt.Exec(
	); err != nil {
		log.Fatal(err)
	}
}

func (a *Aggregator) Execute() {
	if a.tx != nil {
		if err := a.tx.Commit(); err != nil {
			log.Fatal(err)
		}
	}

	defer func(a *Aggregator) {
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
	}(a)
}