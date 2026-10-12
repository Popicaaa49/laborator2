"""Cinci demonstrații de comunicare între procese prin TCP (Python standard)."""

import argparse
import json
import multiprocessing as mp
import socket
import struct
import sys
from datetime import datetime
from pathlib import Path

HOST = "127.0.0.1"
OUTPUT = Path(__file__).resolve().parent / "rezultate"

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def read_exact(connection, size):
    data = bytearray()
    while len(data) < size:
        block = connection.recv(min(65536, size - len(data)))
        if not block:
            raise ConnectionError("Conexiunea s-a închis înainte de primirea datelor.")
        data.extend(block)
    return bytes(data)


def send_packet(address, metadata, payload=b""):
    header = json.dumps({**metadata, "size": len(payload)}).encode("utf-8")
    with socket.create_connection(address, timeout=20) as connection:
        connection.sendall(struct.pack("!I", len(header)))
        connection.sendall(header)
        connection.sendall(payload)


def receive_packet(server):
    connection, _ = server.accept()
    with connection:
        connection.settimeout(20)
        header_size = struct.unpack("!I", read_exact(connection, 4))[0]
        if header_size > 65536:
            raise ValueError("Antet prea mare.")
        metadata = json.loads(read_exact(connection, header_size))
        return metadata, read_exact(connection, metadata["size"])


def worker(kind, ready, settings, destination):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((HOST, 0))
        server.listen(5)
        server.settimeout(20)
        ready.put(server.getsockname())
        if kind == "ring":
            next_address = settings.get(timeout=20)
            metadata, payload = receive_packet(server)
            metadata["route"].append(destination)
            print(f"Proces {destination}: {payload.decode()} | traseu {metadata['route']}", flush=True)
            send_packet(next_address, metadata, payload)
        elif kind == "receive":
            _, payload = receive_packet(server)
            Path(destination).write_bytes(payload)
            print(f"Primit: {destination} ({len(payload)} octeți)", flush=True)
        elif kind == "proxy":
            metadata, payload = receive_packet(server)
            send_packet(destination, metadata, payload)
            print("Proxy: fișier retransmis către destinație.", flush=True)
        elif kind == "merge":
            rows = []
            for _ in range(2):
                metadata, payload = receive_packet(server)
                for line in payload.decode("utf-8").splitlines():
                    timestamp, message = line.split("\t", 1)
                    rows.append((datetime.fromisoformat(timestamp), metadata["source"], message))
            rows.sort(key=lambda row: (row[0], row[1], row[2]))
            Path(destination).write_text("".join(
                f"{stamp.isoformat()}\t{source}\t{message}\n"
                for stamp, source, message in rows
            ), encoding="utf-8")
            print(f"Loguri reunite cronologic: {destination}", flush=True)
        else:
            raise ValueError(f"Rol necunoscut: {kind}")


def start_worker(kind, destination):
    ready, settings = mp.Queue(), mp.Queue()
    process = mp.Process(target=worker, args=(kind, ready, settings, destination))
    process.start()
    try:
        address = ready.get(timeout=20)
    except Exception:
        process.terminate()
        process.join()
        raise
    return process, address, settings


def finish(*processes):
    for process in processes:
        process.join(timeout=25)
        if process.is_alive():
            process.terminate()
            process.join()
            raise TimeoutError("Procesul nu s-a încheiat la timp.")
        if process.exitcode != 0:
            raise RuntimeError(f"Procesul {process.pid} a eșuat: {process.exitcode}")


def demo_ring():
    # Procesul principal este P1; celelalte două procese sunt P2 și P3.
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((HOST, 0))
        server.listen(1)
        server.settimeout(20)
        p3, address3, config3 = start_worker("ring", "P3")
        p2, address2, config2 = start_worker("ring", "P2")
        config2.put(address3)
        config3.put(server.getsockname())
        send_packet(address2, {"route": ["P1"]}, "Salut din P1!".encode())
        metadata, payload = receive_packet(server)
        finish(p2, p3)
        assert metadata["route"] == ["P1", "P2", "P3"]
        assert payload == b"Salut din P1!"
        print("P1: mesajul a revenit. Inel: P1 -> P2 -> P3 -> P1.")


def demo_transfer():
    source = OUTPUT / "sursa.bin"
    source.write_bytes(bytes(range(256)) * 1024)
    target = OUTPUT / "copie.bin"
    receiver, address, _ = start_worker("receive", str(target))
    send_packet(address, {"name": source.name}, source.read_bytes())
    finish(receiver)
    assert target.read_bytes() == source.read_bytes()


def demo_proxy():
    source = OUTPUT / "sursa_proxy.txt"
    source.write_text("Fișier transmis printr-un proces proxy.\n", encoding="utf-8")
    target = OUTPUT / "copie_proxy.txt"
    receiver, address, _ = start_worker("receive", str(target))
    proxy, proxy_address, _ = start_worker("proxy", address)
    send_packet(proxy_address, {"name": source.name}, source.read_bytes())
    finish(proxy, receiver)
    assert target.read_bytes() == source.read_bytes()


def log_sender(address, source, path):
    send_packet(address, {"source": source}, Path(path).read_bytes())


def demo_logs():
    logs = [
        "2026-01-01T10:00:03\tSfârșit A\n2026-01-01T10:00:01\tStart A\n",
        "2026-01-01T10:00:02\tStart B\n2026-01-01T10:00:04\tSfârșit B\n",
    ]
    paths = [OUTPUT / "a.log", OUTPUT / "b.log"]
    for path, content in zip(paths, logs):
        path.write_text(content, encoding="utf-8")
    target = OUTPUT / "reunit.log"
    collector, address, _ = start_worker("merge", str(target))
    senders = [mp.Process(target=log_sender, args=(address, name, str(path)))
               for name, path in zip(("A", "B"), paths)]
    for sender in senders:
        sender.start()
    finish(*senders, collector)
    lines = target.read_text(encoding="utf-8").splitlines()
    stamps = [datetime.fromisoformat(line.split("\t")[0]) for line in lines]
    assert len(lines) == 4 and stamps == sorted(stamps)


def demo_split():
    source = OUTPUT / "de_impartit.bin"
    data = bytes(range(256)) * 1000 + b"final"
    source.write_bytes(data)
    paths = [OUTPUT / f"nod_{index}" / f"fragment_{index}.bin" for index in range(3)]
    receivers = []
    for path in paths:
        path.parent.mkdir(parents=True, exist_ok=True)
        receivers.append(start_worker("receive", str(path)))
    size = (len(data) + len(paths) - 1) // len(paths)
    for index, (_, address, _) in enumerate(receivers):
        send_packet(address, {"index": index, "total": len(paths)},
                    data[index * size:(index + 1) * size])
    finish(*(process for process, _, _ in receivers))
    restored = OUTPUT / "reconstituit.bin"
    restored.write_bytes(b"".join(path.read_bytes() for path in paths))
    assert restored.read_bytes() == data
    print("Cele 3 fragmente au fost distribuite și reconstituite corect.")


DEMOS = {"ring": demo_ring, "transfer": demo_transfer, "proxy": demo_proxy,
         "logs": demo_logs, "split": demo_split}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tema", choices=[*DEMOS, "all"], nargs="?", default="all")
    args = parser.parse_args()
    OUTPUT.mkdir(parents=True, exist_ok=True)
    for name in (DEMOS if args.tema == "all" else [args.tema]):
        print(f"\n=== {name} ===", flush=True)
        DEMOS[name]()
        print("Verificare: OK", flush=True)


if __name__ == "__main__":
    mp.freeze_support()
    main()
