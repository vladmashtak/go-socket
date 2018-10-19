package Clickhouse

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/kshvakov/clickhouse"
)

const (
	NETWORK_INTERFACE_STATISTIC_PREPARED_STATEMENT =
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
	NET_SESSION_PREPARED_STATEMENT =
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
	DNS_PREPARED_STATEMENT =
		`INSERT INTO dns (
			interfaceIndex,
			dpiInstance,
			name,
			ttl,
			count,
			timestamp,
			host
		) VALUES (?,?,?,?,?,?,?)`
	VLAN_PREPARED_STATEMENT =
		`INSERT INTO vlan (
			interfaceIndex,
			dpiInstance,
			vlan
		) VALUES (?,?,?)`
)

func connectClickhouse() (*sql.DB, error)  {
	connect, err := sql.Open("clickhouse", "tcp://127.0.0.1:9000")

	if err != nil {
		log.Fatal(err)
	}

	if err := connect.Ping(); err != nil {
		if exception, ok := err.(*clickhouse.Exception); ok {
			fmt.Printf("[%d] %s \n%s\n", exception.Code, exception.Message, exception.StackTrace)
		} else {
			fmt.Println(err)
		}
		return nil, err
	}

	return connect, nil
}