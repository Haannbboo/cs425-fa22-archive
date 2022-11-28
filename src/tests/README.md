## Hosts and Ports

| Threads    	| Host            	| Port              	| Usage in threads 	|
|:------------	|:-----------------:|:-------------------:	|------------------:|
| introducer 	| SDFS.host       	| PORT_FDINTRODUCER 	| bind             	|
| DNSserver  	| DNS_SERVER_HOST 	| DNS_SERVER_PORT   	| bind             	|
| join       	| FD.host         	| PORT_FDUPDATE     	| bind             	|
| receiver      | FD.host           | FD.port               | bind              |

## Messages

### join

1. ``GET_INTRODUCER``: FD.join -- sendto -> (DNS_SERVER_HOST, DNS_SERVER_PORT)
2. ``RESP INTRODUCER`` DNS -- sendto -> FD.join
3. ``JOIN`` FD.join -- sendto -> (introducer_host = SDFS.host, introducer_port = PORT_FDINTRODUCER)
4. ``UPDATE`` introducer -- sendto -> FD.join
5. ``JOIN`` FD.join -> neighbor(s) (host = ml[i].host, port = ml[i].port)