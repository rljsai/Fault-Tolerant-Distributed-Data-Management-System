import asyncio
import aiohttp
from aiodocker import Docker
from hash_ring import HashRing
from colorama import Fore, Style
import asyncpg
import os

class Manager:
    def __init__(self, heartbeat_interval=5, max_fails=3, on_server_dead=None, db_pool=None):
        self.ring = HashRing()
        self.replicas = set()
        self.heartbeat_fail_count = {}
        self.semaphore = asyncio.Semaphore(5)  # limit concurrent Docker ops
        self.heartbeat_interval = heartbeat_interval
        self.max_fails = max_fails
        self.counter = 1  # auto-server names
        self._task = None  # heartbeat checker
        self.on_server_dead = on_server_dead
        self.db_pool = db_pool  # asyncpg pool for LB DB

    async def start(self):
        if not self._task:
            self._task = asyncio.create_task(self._heartbeat_checker())

    async def stop(self):
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None
        for h in list(self.replicas):
            await self.remove_server(h)

    async def spawn_server(self, hostname: str, shards=None):
        """
        Spawn a server container and optionally assign shards.
        """
        async with self.semaphore:
            async with Docker() as docker:
                container = await docker.containers.create_or_replace(
                    name=hostname,
                    config={
                        "Image": "myserver",
                        "Env": [
                            f"SERVER_ID={hostname}",
                            "POSTGRES_USER=postgres",
                            "POSTGRES_PASSWORD=postgres",
                            "POSTGRES_DB=studdb",
                        ],
                        "Hostname": hostname,
                        "Tty": True,
                    }
                )
                # Connect to network net1 if exists
                try:
                    net = await docker.networks.get("net1")
                    await net.connect({
                        "Container": container.id,
                        "EndpointConfig": {"Aliases": [hostname]}
                    })
                except Exception:
                    pass
                await container.start()
                print(f"{Fore.GREEN}[Spawned]{Style.RESET_ALL} {hostname}")

        self.replicas.add(hostname)
        self.ring.add_server(hostname)
        self.heartbeat_fail_count[hostname] = 0

        # Configure server with shards if provided
        if shards and self.db_pool:
            async with aiohttp.ClientSession() as session:
                try:
                    async with session.post(
                        f"http://{hostname}:5000/config",
                        json={"shards": shards}
                    ) as resp:
                        if resp.status == 200:
                            print(f"[Config] {hostname} configured with {shards}")
                        else:
                            print(f"[Config] {hostname} /config failed, status={resp.status}")
                except Exception as e:
                    print(f"[Config] Error configuring {hostname}: {e}")

            # Update LB DB metadata to include this server in shards
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    for shard in shards:
                        await conn.execute(
                            "UPDATE ShardT SET servers=array_append(servers, $1) WHERE shard_id=$2 AND NOT ($1 = ANY(servers))",
                            hostname, shard
                        )

    async def remove_server(self, hostname: str):
        async with self.semaphore:
            async with Docker() as docker:
                try:
                    container = await docker.containers.get(hostname)
                    await container.stop(timeout=3)
                    await container.delete(force=True)
                    print(f"{Fore.YELLOW}[Removed]{Style.RESET_ALL} {hostname}")
                except Exception:
                    pass

        if hostname in self.replicas:
            self.replicas.remove(hostname)
            self.ring.remove_server(hostname)
            self.heartbeat_fail_count.pop(hostname, None)

        # Remove server from LB DB metadata
        if self.db_pool:
            async with self.db_pool.acquire() as conn:
                async with conn.transaction():
                    await conn.execute(
                        "UPDATE ShardT SET servers=array_remove(servers, $1)",
                        hostname
                    )

    def list_servers(self):
        return {
            "N": len(self.replicas),
            "replicas": list(self.replicas)
        }

    def get_server_for_request(self, rid: int):
        return self.ring.get_server(rid)

    # ---------------- Heartbeat ----------------
    async def _heartbeat_checker(self):
        while True:
            await asyncio.sleep(self.heartbeat_interval)
            dead = []
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=2)) as session:
                for server in list(self.replicas):
                    try:
                        async with session.get(f"http://{server}:5000/heartbeat") as resp:
                            if resp.status != 200:
                                raise Exception("bad heartbeat")
                            self.heartbeat_fail_count[server] = 0
                    except Exception:
                        self.heartbeat_fail_count[server] = (
                            self.heartbeat_fail_count.get(server, 0) + 1
                        )
                        if self.heartbeat_fail_count[server] >= self.max_fails:
                            dead.append(server)

            for d in dead:
                print(f"{Fore.RED}[Heartbeat] {d} failed! Respawning...{Style.RESET_ALL}")
                if self.on_server_dead:
                    await self.on_server_dead(d)
