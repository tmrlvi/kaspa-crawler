#!/usr/bin/env python
import logging

import os
import grpc
import time
import random
import ipaddress
import asyncio
import json
import aiohttp
import resource

import p2p_pb2, messages_pb2, messages_pb2_grpc


async def message_stream(queue):
    message = await queue.get()
    while message is not None:
        logging.debug("Sending %s", message)
        yield message
        queue.task_done()
        message = await queue.get()
    queue.task_done()

class P2PNode(object):
    USER_AGENT = "/crawler:0.0.1/"

    def __init__(self, address='localhost:16111', network="kaspa-mainnet", ipinfo_token = None):
        self.network = network
        self.address = address
        self.ipinfo_token = f"?token={ipinfo_token}" if ipinfo_token is not None else ""
        
    async def ipinfo(self, session, semaphore: asyncio.Semaphore):
        addr, _ = self.address.rsplit(":",1)
        addr = addr.replace("ipv6:","").strip("[]")
        while True:
            try:
                async with semaphore:
                    async with session.get(f"https://ipinfo.io/{addr}{self.ipinfo_token}") as response:
                        resp = await response.json()
                        if "loc" not in resp:
                            logging.warning(f"IPInfo response is missing location for {addr}: {resp}")
                        return resp.get("loc", "")
            except OSError as e:
                logging.warning(f"Error reading ipinfo: {e}")
                await asyncio.sleep(int(2*random.random())*10)

    async def __aenter__(self):
        self.ID = bytes.fromhex(hex(int(random.random()*10000))[2:].zfill(32))
        self.channel = grpc.aio.insecure_channel(self.address)

        self.peer_version = 2
        self.peer_id = None

        await asyncio.wait_for(self.channel.channel_ready(), 2)
        self.stub = messages_pb2_grpc.P2PStub(self.channel)

        self.send_queue = asyncio.queues.Queue()

        self.stream = self.stub.MessageStream(message_stream(self.send_queue))
        self.stream.address = self.address
        await self.handshake()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.send_queue.put(None)
        if exc_type is not None and issubclass(exc_type, asyncio.CancelledError):
            self.stream.cancel()
        else:
            await self.send_queue.join()
        await self.channel.close(2)
        pass

    async def handshake(self):
        logging.debug("Starting handshake")
        async for item in self.stream:
            logging.debug("Getting %s", item)
            payload = item.WhichOneof("payload")
            if payload == "version":
                self.peer_id = item.version.id
                self.peer_version = item.version.protocolVersion
                self.peer_kaspad = item.version.userAgent
                await self.send_queue.put(messages_pb2.KaspadMessage(version=p2p_pb2.VersionMessage(
                    protocolVersion=self.peer_version,
                    timestamp=int(time.time()),
                    id=self.ID,
                    userAgent=self.USER_AGENT,
                    network=self.network
                )))
            elif payload == "verack":
                await self.send_queue.put(messages_pb2.KaspadMessage(verack=p2p_pb2.VerackMessage()))
                if self.peer_version < 4:
                    logging.debug("Handshake done")
                    return
            elif payload == "ready":
                await self.send_queue.put(messages_pb2.KaspadMessage(ready=p2p_pb2.ReadyMessage()))
                logging.debug("Handshake done")
                return
            else:
                logging.debug("During handshake, got unexpected %s", payload)

        raise ConnectionError("Wrong protocol")

    async def get_addresses(self):
        logging.debug("Starting get_addresses")
        await self.send_queue.put(messages_pb2.KaspadMessage(requestAddresses=p2p_pb2.RequestAddressesMessage()))
        async for item in self.stream:
            logging.debug("Getting %s", item)
            payload = item.WhichOneof("payload")
            if payload == "addresses":
                return item.addresses.addressList
            elif payload == "requestAddresses":
                #self.send_queue.put(messages_pb2.KaspadMessage(reject=p2p_pb2.RejectMessage(reason="No Addresses (Bot)")))
                await self.send_queue.put(
                    messages_pb2.KaspadMessage(addresses=p2p_pb2.AddressesMessage(addressList=[])))
            else:
                pass
        #raise RuntimeError("Failed getting addresses: %s" % self.address)


async def get_addresses(address, network, semaphore: asyncio.Semaphore, ipinfo_token=None):
    try:
        addresses = set()
        prev_size = -1
        patience = 10
        peer_id = ""
        peer_kaspad = ""
        loc = ""
        try:
            await asyncio.sleep(0)
            async with aiohttp.ClientSession() as session:
                async with P2PNode(address, network, ipinfo_token=ipinfo_token) as node:
                    await asyncio.sleep(0)
                    peer_id = node.peer_id.hex()
                    peer_kaspad = node.peer_kaspad
                    prev = time.time()
                    while len(addresses) > prev_size or patience > 0:
                        # Log info every 5 seconds approximately
                        if time.time() - prev > 5:
                            await asyncio.sleep(0)
                            logging.info("getting more addresses")
                            prev = time.time()
                        if len(addresses) <= prev_size:
                            patience -= 1
                        else:
                            patience = 10
                        prev_size = len(addresses)
                        item = await node.get_addresses()
                        if item is not None:
                            addresses.update(((x.timestamp, x.ip, x.port) for x in item))
                    await asyncio.sleep(0)
                    loc = await node.ipinfo(session, semaphore)
        except asyncio.exceptions.TimeoutError as e:
            logging.debug("Node %s timed out", address)
            return address, peer_id, peer_kaspad, addresses, "timeout", loc
        except Exception as e:
            logging.exception("Error in task")
            return address, peer_id, peer_kaspad, addresses, e, loc
        return address, peer_id, peer_kaspad, addresses, "", loc
    except asyncio.CancelledError:
        logging.debug("Task was canceled")


async def main(addresses, network, output, ipinfo_token=None):
    ulimit, _ = resource.getrlimit(resource.RLIMIT_NOFILE)
    ulimit = max(ulimit-20, 1)
    #ulimit = max(ulimit - resource.getrusage(resource.RLIMIT_NOFILE)//2, 1)
    logging.info(f"Running {ulimit} tasks concurrently")
    semaphore = asyncio.Semaphore(ulimit)

    res = {}
    bad_ipstrs = []
    seen = set()
    pending = [asyncio.create_task(get_addresses(f"{address}:{port}", network, semaphore, ipinfo_token=ipinfo_token)) for address, port in addresses]
    try:
        while len(pending) > 0:
            logging.info(f"Currently pending: {len(pending)}")
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

            for task in done:
                address, peer_id, peer_kaspad, addresses, error, loc = task.result()
                res[address] = { "neighbors": [], "id": peer_id, "kaspad": peer_kaspad, "error": error, "loc": loc }
                if error is not None:
                    res[address]["error"] = repr(error)
                for ts, ipstr, port in addresses:
                    if ipstr.hex() not in bad_ipstrs:
                        try:
                            ip = ipaddress.ip_address(ipstr)
                            if not ip.is_private and not ip.is_loopback:
                                if isinstance(ip, ipaddress.IPv6Address):
                                    new_address = f"ipv6:[{ip}]:{port}"
                                else:
                                    new_address = f"{ip}:{port}"
                                res[address]["neighbors"].append(new_address)
                                if new_address not in seen:
                                    seen.add(new_address)
                                    pending.add(asyncio.create_task(get_addresses(new_address, network, semaphore, ipinfo_token=ipinfo_token)))
                            else:
                                logging.debug(f"Got private address {ip}")
                        except Exception as e:
                            logging.exception("Bad ip")
                            bad_ipstrs.append(ipstr.hex())
        logging.info(f"Done")
    finally:
        for task in pending:
            task.cancel()

        logging.info("Writing results...")
        async with semaphore:
            json.dump(res, open(output, "w"), allow_nan=False, indent=2, sort_keys=True, ensure_ascii=True)

        while len(pending) > 0:
            logging.warning(f"Shutting down after cancelling {len(pending)} tasks. Please wait...")
            done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)
        logging.warning(f"All tasks seem to be down. Finalizing shut down...")

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Crawler to list all known p2p nodes and their information. Used to create a map of the p2p nodes"
    )
    parser.add_argument("-v", "--verbose", help="Verbosity level", action='count', default=0)
    parser.add_argument("--addr", help="Start ip:port for crawling", default="seeder1.kaspad.net:16111")
    parser.add_argument("--output", help="output json path", default="data/nodes.json")
    parser.add_argument("--network", help="Which network to connect to",
                        choices=["kaspa-mainnet", "kaspa-testnet", "kaspa-devnet"], default="kaspa-mainnet")
    parser.add_argument("--token", help="IP info token")

    args = parser.parse_args()

    if not (
        os.access(args.output, os.W_OK) or
        (not os.path.exists(args.output) and os.access(os.path.dirname(args.output), os.W_OK))
    ):
        parser.error(f"Cannot write to {args.output} (check directory exists and you have permissions)")

    logging.basicConfig(level=[logging.WARN, logging.INFO, logging.DEBUG][min(args.verbose, 2)])
    hostpair = args.addr.split(":") if ":" in args.addr else (args.addr, "16111")

    asyncio.run(main([hostpair], args.network, args.output, ipinfo_token=args.token))
