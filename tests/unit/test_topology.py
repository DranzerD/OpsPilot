from sim.config import load_topology


def test_dependencies_reference_known_components():
    topo = load_topology()
    known = set(topo.services) | set(topo.databases)
    for caller, callee in topo.edges():
        assert callee in known, f"{caller} calls unknown {callee}"


def test_ports_unique():
    ports = [s.port for s in load_topology().services.values()]
    assert len(ports) == len(set(ports))


def test_graph_is_acyclic():
    topo = load_topology()
    graph: dict[str, list[str]] = {}
    for caller, callee in topo.edges():
        graph.setdefault(caller, []).append(callee)

    visiting, done = set(), set()

    def visit(node: str) -> None:
        assert node not in visiting, f"cycle through {node}"
        if node in done:
            return
        visiting.add(node)
        for nxt in graph.get(node, []):
            visit(nxt)
        visiting.discard(node)
        done.add(node)

    for node in graph:
        visit(node)


def test_http_dependencies_have_timeouts():
    topo = load_topology()
    for svc in topo.services.values():
        for dep in svc.calls:
            if dep.name in topo.services:
                assert dep.timeout_s, f"{svc.name} -> {dep.name} has no timeout"
